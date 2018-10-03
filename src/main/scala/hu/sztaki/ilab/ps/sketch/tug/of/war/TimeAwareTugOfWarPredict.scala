package hu.sztaki.ilab.ps.sketch.tug.of.war

import hu.sztaki.ilab.ps.entities.{PSToWorker, Pull, Push, WorkerToPS}
import hu.sztaki.ilab.ps.sketch.tug.of.war.pslogic.TimeAwareToWPredictPSLogic
import hu.sztaki.ilab.ps.{FlinkParameterServer, ParameterServerClient, WorkerLogic}
import org.apache.flink.streaming.api.scala._
import hu.sztaki.ilab.ps.sketch.utils.Utils.Vector
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class TimeAwareTugOfWarPredict {

}

object TimeAwareTugOfWarPredict {

  def predict(src: DataStream[(Int, String)],
              model: DataStream[(Int, Either[((Int, Int),Vector),  (Int, Vector)])],
              numHashes: Int,
              numMeans: Int,
              K: Int,
              workerParallelism: Int,
              psParallelism: Int,
              pullLimit: Int,
              iterationWaitTime: Long): DataStream[((Int, Int), Array[(Double, Int)])] = {

    val workerLogic = new WorkerLogic[(Int, String), Int, Either[((Int, Int),Vector),  (Int, Vector)], Any] {

      val queryBuffer = new mutable.HashMap[Int, Int]()

      override def onRecv(data: (Int, String),
                          ps: ParameterServerClient[Int, Either[((Int, Int), Vector), (Int, Vector)], Any]): Unit = {
        queryBuffer.update(data._2.hashCode, data._1)
        ps.pull(data._2.hashCode)
      }

      override def onPullRecv(paramId: Int, paramValue: Either[((Int, Int), Vector), (Int, Vector)],
                              ps: ParameterServerClient[Int, Either[((Int, Int), Vector), (Int, Vector)], Any]): Unit = {
        paramValue match {
          case Left(((_, timeSlot), targetVector)) =>

            for(i <- 0 until psParallelism){
              ps.push(i, Left((queryBuffer(paramId), timeSlot), targetVector))
            }
          case Right(_) =>
        }
      }
    }

    val serverLogic = new TimeAwareToWPredictPSLogic(numHashes, numMeans, K)

    val hashFunc: Any => Int = x => Math.abs(x.hashCode())

    val workerToPSPartitioner: WorkerToPS[Int, Either[((Int, Int), Vector),  (Int, Vector)]] => Int = {
      case WorkerToPS(_, msg) =>
        msg match {
          case Left(Pull(pId)) => hashFunc(pId) % psParallelism
          case Right(Push(pId, _)) => hashFunc(pId) % psParallelism
        }
    }

    val psToWorkerPartitioner: PSToWorker[Int, Either[((Int, Int), Vector),  (Int, Vector)]] => Int = {
      case PSToWorker(workerPartitionIndex, _) => workerPartitionIndex
    }

    val predict = FlinkParameterServer.transformWithModelLoad(model)(
      src,
      WorkerLogic.addPullLimiter(workerLogic, pullLimit),
      serverLogic,
      workerToPSPartitioner,
      psToWorkerPartitioner,
      workerParallelism,
      psParallelism,
      iterationWaitTime)

    predict.flatMap(new RichFlatMapFunction[Either[Any, ((Int, Int), Array[(Double, Int)])], ((Int, Int), Array[(Double, Int)])] {
      val buffer = new mutable.HashMap[(Int, Int), ArrayBuffer[Array[(Double, Int)]]]()

      def everythingArrived(allTopK: ArrayBuffer[Array[(Double, Int)]]): Boolean = allTopK.size == psParallelism


      override def flatMap(value: Either[Any, ((Int, Int), Array[(Double, Int)])],
                           out: Collector[((Int, Int), Array[(Double, Int)])]): Unit = {
        value match {
          case Left(_) =>

          case Right((queryId, localTopK)) =>
            buffer.getOrElseUpdate(queryId, new ArrayBuffer[Array[(Double, Int)]]()) += localTopK

            if(everythingArrived(buffer(queryId))){
              val topK = new ArrayBuffer[(Double, Int)]()

              for(a <- buffer(queryId)){
                topK ++= a
              }

              out.collect( (queryId, topK.sorted.takeRight(K).reverse.toArray))
            }
        }
      }
    }).setParallelism(1)
  }
}
