package hu.sztaki.ilab.ps.sketch.minhash.variation

import hu.sztaki.ilab.ps.sketch.minhash.pslogic.StoredHashPSLogic
import hu.sztaki.ilab.ps.{FlinkParameterServer, ParameterServerClient, WorkerLogic}
import org.apache.flink.streaming.api.scala._


class MinHash {

}


object MinHash {


  def minhash(src: DataStream[(String, Array[String])],
              numHashes: Int,
              workerParallelism: Int,
              psParallelism: Int,
              iterationWaitTime: Long) : DataStream[(Int, Array[Long])] = {

    val workerLogic = new WorkerLogic[(String, Array[String]), Int, Long, Array[String]] {

      /**
      * Method called when new data arrives.
      *
      * @param tweet
      * New data.
      * @param ps
      * Interface to ParameterServer.
      */
      override def onRecv(tweet: (String, Array[String]), ps: ParameterServerClient[Int, Long, Array[String]]): Unit =
        for (word <- tweet._2) {
          ps.push(word.hashCode, tweet._1.toLong)
        }

      /**
        * Method called when an answer arrives to a pull message.
        * It contains the parameter.
        *
        * @param paramId
        * Identifier of the received parameter.
        * @param paramValue
        * Value of the received parameter.
        * @param ps
        * Interface to ParameterServer.
        */
      override def onPullRecv(paramId: Int,
                              paramValue: Long,
                              ps: ParameterServerClient[Int, Long, Array[String]]): Unit = ???
    }

    val serverLogic = new StoredHashPSLogic(numHashes)


    FlinkParameterServer.transform(
      src,
      workerLogic,
      serverLogic,
      workerParallelism,
      psParallelism,
      iterationWaitTime)
      .flatMap(_ match {
        case Left(_) => None
        case Right(param) => Some(param)
      })
  }
}
