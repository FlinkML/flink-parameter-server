package hu.sztaki.ilab.ps.sketch.tug.of.war

import hu.sztaki.ilab.ps.sketch.utils.Utils._
import hu.sztaki.ilab.ps.sketch.tug.of.war.pslogic.SketchPredictPSLogic
import hu.sztaki.ilab.ps.entities.{PSToWorker, Pull, Push, WorkerToPS}
import hu.sztaki.ilab.ps.{FlinkParameterServer, ParameterServerClient, WorkerLogic}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object TugOfWarPredict {

  def tugOfWarPredict(src: DataStream[(Int, String)],
                      model: DataStream[(Int, Either[(Int, Vector), Vector])],
                      numHashes: Int,
                      numMeans: Int,
                      K: Int,
                      workerParallelism: Int,
                      psParallelism: Int,
                      pullLimit: Int,
                      iterationWaitTime: Long): DataStream[(Int, Array[(Double, Int)])] = {


    val workerLogic = new WorkerLogic[(Int, String), Int, Either[(Int, Vector), Vector], Any] {

      val queryBuffer = new mutable.HashMap[Int, Int]()

      override def onRecv(data: (Int, String),
                          ps: ParameterServerClient[Int, Either[(Int, Vector), Vector], Any]): Unit = {
        queryBuffer.update(data._2.hashCode, data._1)
        ps.pull(data._2.hashCode)
      }

      override def onPullRecv(paramId: Int,
                              paramValue:  Either[(Int, Vector), Vector],
                              ps: ParameterServerClient[Int, Either[(Int, Vector), Vector], Any]): Unit = {

        paramValue match {
          case Left((_, targetVector)) =>
            for(i <- 0 until psParallelism){
              ps.push(i, Left((queryBuffer(paramId), targetVector)))
            }

          case Right(_) =>
        }

      }
    }

    val hashFunc: Any => Int = x => Math.abs(x.hashCode())

    val serverLogic = new SketchPredictPSLogic(numHashes, numMeans, K)


    val workerToPSPartitioner: WorkerToPS[Int, Either[(Int, Vector), Vector]] => Int = {
      case WorkerToPS(_, msg) =>
        msg match {
          case Left(Pull(pId)) => hashFunc(pId) % psParallelism
          case Right(Push(pId, _)) => hashFunc(pId) % psParallelism
        }
    }

    val psToWorkerPartitioner: PSToWorker[Int, Either[(Int, Vector), Vector]] => Int = {
      case PSToWorker(workerPartitionIndex, _) => workerPartitionIndex
    }


    val eval = FlinkParameterServer.transformWithModelLoad(model)(src,
      workerLogic = WorkerLogic.addPullLimiter(workerLogic, pullLimit),
      serverLogic,
      workerToPSPartitioner,
      psToWorkerPartitioner,
      workerParallelism,
      psParallelism,
      iterationWaitTime)

    eval.flatMap(new RichFlatMapFunction[Either[Any, (Int, Array[(Double, Int)])], (Int, Array[(Double, Int)])] {
      val buffer = new mutable.HashMap[Int, ArrayBuffer[Array[(Double, Int)]]]()

      def everythingArrived(allTopK: ArrayBuffer[Array[(Double, Int)]]): Boolean = allTopK.size == psParallelism


      override def flatMap(value: Either[Any, (Int, Array[(Double, Int)])], out: Collector[(Int, Array[(Double, Int)])]): Unit = {

        value match {
          case Left(_) =>

          case Right((queryId, localTopK)) =>
            buffer.getOrElseUpdate(queryId, new ArrayBuffer[Array[(Double, Int)]]()) += localTopK

            if(everythingArrived(buffer(queryId))){
              val topK = new ArrayBuffer[(Double, Int)]()

              for(a <- buffer(queryId)){
                topK ++= a
              }

              out.collect( (queryId, topK.sortWith(_._1 > _._1).take(K).toArray))
            }
        }
      }
    }).setParallelism(1)

  }
}