package hu.sztaki.ilab.ps.sketch.minhash

import hu.sztaki.ilab.ps.sketch.minhash.pslogic.SendHashPSLogic
import hu.sztaki.ilab.ps.sketch.utils.Utils.Vector
import hu.sztaki.ilab.ps.{FlinkParameterServer, ParameterServerClient, WorkerLogic}
import org.apache.flink.streaming.api.scala._


/**
  * MinHash (https://en.wikipedia.org/wiki/MinHash) algorithm on top of Parameter Server
  */
object MinHash {


  def minhash(src: DataStream[(String, Array[String])],
              numHashes: Int,
              workerParallelism: Int,
              psParallelism: Int,
              iterationWaitTime: Long) : DataStream[(Int, Array[Long])] = {

    val workerLogic = new WorkerLogic[(String, Array[String]), Int, (Long, Vector), Array[String]] {

      /**
        * Method called when new data arrives.
        *
        * @param tweet
        * New data.
        * @param ps
        * Interface to ParameterServer.
        */
      override def onRecv(tweet: (String, Array[String]), ps: ParameterServerClient[Int, (Long, Vector), Array[String]]): Unit = {
        val (id, tweetText) = tweet

        val a = new Vector(numHashes)
        (0 until numHashes).foreach(i =>
          a(i) = scala.util.hashing.MurmurHash3.stringHash(id, i)
        )

        for(word <- tweetText) {
          ps.push(word.hashCode, (id.toLong, a))
        }
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
                              paramValue: (Long, Vector),
                              ps: ParameterServerClient[Int, (Long, Vector), Array[String]]): Unit = ???
    }

    val serverLogic = new SendHashPSLogic(numHashes)


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
