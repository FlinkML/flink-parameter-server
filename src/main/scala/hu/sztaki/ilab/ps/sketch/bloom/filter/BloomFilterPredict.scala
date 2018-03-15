package hu.sztaki.ilab.ps.sketch.bloom.filter

import hu.sztaki.ilab.ps.entities.{PSToWorker, Pull, Push, WorkerToPS}
import hu.sztaki.ilab.ps.sketch.bloom.filter.pslogic.BloomPredictPSLogic
import hu.sztaki.ilab.ps.{FlinkParameterServer, ParameterServerClient, WorkerLogic}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
  * Load a previously trained bloom filter and make predictions for common occurrence
  */

object BloomFilterPredict {

  /**
    *
    * @param src: Stream of datapoints, where the first element is the ID of the request and the second is the value
    * @param model: Stream of the model
    * @param arraySize: Parameter of the algorithm - m
    * @param numHashes: Parameter of the algorithm - k
    * @param K: Size of the return list (top-K)
    * @param workerParallelism: Number of worker nodes for the parameter server
    * @param psParallelism: Number of server nodes for the parameter server
    * @param pullLimit: Maximum number of unanswered pull requests in the system
    * @param iterationWaitTime:  Parameter for Loop API
    * @return : The top-k results for the requests in the form of (request ID, (word ID, #commonoccurrences))
    */
  def predict(src: DataStream[(Int, String)],
              model: DataStream[(Int, Either[(Int, mutable.BitSet), mutable.BitSet])],
              arraySize: Int,
              numHashes: Int,
              K: Int,
              workerParallelism: Int,
              psParallelism: Int,
              pullLimit: Int,
              iterationWaitTime: Long) : DataStream[(Int, Array[(Double, Int)])] = {


    val workerLogic = new WorkerLogic[(Int, String),  Either[(Int, mutable.BitSet), mutable.BitSet], Any] {

      val queryBuffer = new mutable.HashMap[Int, Int]()

      /**
      * Method called when new data arrives.
      *
      * @param data
      * New data.
      * @param ps
      * Interface to ParameterServer.
      */
    override def onRecv(data: (Int, String), ps: ParameterServerClient[Either[(Int, mutable.BitSet), mutable.BitSet], Any]): Unit = {
      queryBuffer.update(data._2.hashCode, data._1)
      ps.pull(data._2.hashCode)
    }

      /**
        * When the server answered with requested vector it will be broadcasted for all server nodes, where the top-k are calculated
        *
        * @param paramId
        * Identifier of the received parameter.
        * @param paramValue
        * Value of the received parameter.
        * @param ps
        * Interface to ParameterServer.
        */
      override def onPullRecv(paramId: Int, paramValue:Either[(Int, mutable.BitSet), mutable.BitSet],
                              ps: ParameterServerClient[Either[(Int, mutable.BitSet), mutable.BitSet], Any]): Unit = {

        paramValue match {
          case Left((_, targetVector)) =>
            for(i <- 0 until psParallelism){
              ps.push(i, Left((queryBuffer(paramId), targetVector)))
            }

          case Right(_) =>
        }

      }
    }

    val serverLogic = new BloomPredictPSLogic(arraySize, numHashes, K)


    val hashFunc: Any => Int = x => Math.abs(x.hashCode())

    val workerToPSPartitioner: WorkerToPS[ Either[(Int, mutable.BitSet), mutable.BitSet]] => Int = {
      case WorkerToPS(_, msg) =>
        msg match {
          case Left(Pull(pId)) => hashFunc(pId) % psParallelism
          case Right(Push(pId, _)) => hashFunc(pId) % psParallelism
        }
    }

    val psToWorkerPartitioner: PSToWorker[ Either[(Int, mutable.BitSet), mutable.BitSet]] => Int = {
      case PSToWorker(workerPartitionIndex, _) => workerPartitionIndex
    }


    val predict = FlinkParameterServer.transformWithModelLoad(model)(src,
      workerLogic = WorkerLogic.addPullLimiter(workerLogic, pullLimit),
      serverLogic,
      workerToPSPartitioner,
      psToWorkerPartitioner,
      workerParallelism,
      psParallelism,
      iterationWaitTime)

    predict.flatMap(new RichFlatMapFunction[Either[Any, (Int, Array[(Double, Int)])], (Int, Array[(Double, Int)])] {
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

              out.collect( (queryId, topK.sorted.takeRight(K).reverse.toArray))
            }
        }
      }
    }).setParallelism(1)
  }

}