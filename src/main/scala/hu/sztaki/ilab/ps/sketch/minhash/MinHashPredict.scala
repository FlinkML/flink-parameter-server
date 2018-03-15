package hu.sztaki.ilab.ps.sketch.minhash

import hu.sztaki.ilab.ps.entities.{PSToWorker, Pull, Push, WorkerToPS}
import hu.sztaki.ilab.ps.sketch.minhash.pslogic.MinHashPredictPSLogic
import hu.sztaki.ilab.ps.{FlinkParameterServer, ParameterServerClient, WorkerLogic}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.mutable

class MinHashPredict

object MinHashPredict {
  private val log = LoggerFactory.getLogger(classOf[MinHashPredict])

  private def wordCount(input: DataStream[(String, Array[String])]) =
    input.flatMap(_._2.map((_, 1))).keyBy(0).flatMap(new CountWords)

  class CountWords extends RichFlatMapFunction[(String, Int), (String, Int)] {
    var words: mutable.HashMap[String, Int] = mutable.HashMap.empty[String, Int]
    private var collector: Option[Collector[(String, Int)]] = None

    override def flatMap(value: (String, Int), out: Collector[(String, Int)]): Unit = {
      if(collector.isEmpty) collector = Some(out)
      words += ((value._1, words.getOrElse(value._1, 0) +1))
    }


    override def close(): Unit = collector match {
      case Some(out) => words.foreach(out.collect)
        log.info(getRuntimeContext.getIndexOfThisSubtask + " > size:" + words.size)
      case None =>  log.error("Collector is empty.")
    }
  }

  class Aggregate extends RichCoFlatMapFunction[(Int, Array[(Double, Int)]), (String, Int), (Int, Array[(Int, Long)])] {
    private val wordsCount = mutable.HashMap.empty[Int, Int]
    private val res = mutable.Queue[(Int, Array[(Double, Int)])]()
    private var collector: Option[Collector[(Int, Array[(Int, Long)])]] = None

    override def flatMap2(value: (String, Int), out: Collector[(Int, Array[(Int, Long)])]): Unit =
      wordsCount += value._1.hashCode -> value._2

    override def flatMap1(value: (Int, Array[(Double, Int)]), out: Collector[(Int, Array[(Int, Long)])]): Unit = {
      if(collector.isEmpty) collector = Some(out)
      res.enqueue(value)
    }

    override def close(): Unit = collector match {
      case Some(out) =>
        res.foreach { case (wordHash, intersections) =>
          val freq = wordsCount.getOrElse(wordHash, 0)
          val res = intersections.map { case (value, word) =>
            val tmpFreq = wordsCount.getOrElse(word, 0)
            (word, Math.round((value * (freq + tmpFreq)) / (value + 1)))
          }
          out.collect((wordHash, res.sortBy(_._2).reverse))
        }
      case None =>  log.error("Collector is empty.")
    }


  }

  def minhashPredict(words: DataStream[String],
                     train: DataStream[(String, Array[String])],
                     model: DataStream[(Int, Either[(Int, Array[Long]), Array[Long]])],
                     numHashes: Int,
                     K: Int,
                     workerParallelism: Int,
                     psParallelism: Int,
                     pullLimit: Int,
                     iterationWaitTime: Long): DataStream[(Int, Array[(Int, Long)])] = {

    val searchWords = words.forward.map(_.hashCode)
    val workerLogic = new WorkerLogic[Int,  Either[(Int, Array[Long]), Array[Long]], Any] {

      override def onRecv(wordHash: Int, ps: ParameterServerClient[Either[(Int, Array[Long]), Array[Long]], Any]):
      Unit =
        ps.pull(wordHash)

      override def onPullRecv(paramId: Int,
                              paramValue:  Either[(Int, Array[Long]), Array[Long]],
                              ps: ParameterServerClient[ Either[(Int, Array[Long]), Array[Long]], Any]):
      Unit =
        paramValue match {
          case Right(targetVector) =>
            (0 until psParallelism) foreach (i => ps.push(i, Left((paramId, targetVector))))
          case _ => throw new IllegalStateException("PS should not send Left pull answers")
        }
    }

    val hashFunc: Any => Int = x => Math.abs(x.hashCode())

    val serverLogic = new MinHashPredictPSLogic(numHashes, K)


    val workerToPSPartitioner: WorkerToPS[ Either[(Int, Array[Long]), Array[Long]]] => Int = {
      case WorkerToPS(_, msg) =>
        msg match {
          case Left(Pull(pId)) => hashFunc(pId) % psParallelism
          case Right(Push(pId, _)) => hashFunc(pId) % psParallelism
        }
    }

    val psToWorkerPartitioner: PSToWorker[ Either[(Int, Array[Long]), Array[Long]]] => Int = {
      case PSToWorker(workerPartitionIndex, _) => workerPartitionIndex
    }


    val res = FlinkParameterServer.transformWithModelLoad(model)(searchWords,
      workerLogic = WorkerLogic.addPullLimiter(workerLogic, pullLimit),
      serverLogic,
      workerToPSPartitioner,
      psToWorkerPartitioner,
      workerParallelism,
      psParallelism,
      iterationWaitTime).keyBy{_ match {
      case Right((wordHash, _)) => wordHash
      case _ => 0
    }}.flatMap(new RichFlatMapFunction[Either[Any, (Int, Array[(Double, Int)])], (Int, Array[(Double, Int)])] {
      val buffer = new mutable.HashMap[Int, (Int, Array[(Double, Int)])]()

      override def flatMap(value: Either[Any, (Int, Array[(Double, Int)])], out: Collector[(Int, Array[(Double, Int)])]):
      Unit =
        value match {
          case Right((queryId, localTopK)) =>
            val (counter, localTopKs) = buffer.getOrElse(queryId, (0, Array.empty[(Double, Int)]))
            if(counter + 1 >= psParallelism){
              out.collect( (queryId, localTopKs ++ localTopK))
            } else {
              buffer += (queryId -> (counter + 1, localTopKs ++ localTopK))
            }
          case _ => throw new IllegalStateException("PS should not send Left pull answers")
        }}).setParallelism(psParallelism)
    val count = wordCount(train).setParallelism(psParallelism)
    res.connect(count).flatMap(new Aggregate).setParallelism(1)
  }
}