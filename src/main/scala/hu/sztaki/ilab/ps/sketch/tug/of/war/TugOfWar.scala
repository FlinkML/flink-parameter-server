package hu.sztaki.ilab.ps.sketch.tug.of.war

import hu.sztaki.ilab.ps.sketch.utils.Utils._
import hu.sztaki.ilab.ps.sketch.tug.of.war.pslogic.BitSetBasedPSLogic
import hu.sztaki.ilab.ps.{FlinkParameterServer, ParameterServerClient, WorkerLogic}
import net.openhft.hashing.LongHashFunction
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Tug of War sketch on top of Parameter Server
  */
object TugOfWar {


  def tugOfWar(src: DataStream[(String, Array[String])],
               numHashes: Int,
               workerParallelism: Int,
               psParallelism: Int,
               iterationWaitTime: Long) : DataStream[(Int, Vector)] = {

    val workerLogic = new WorkerLogic[(String, Array[String]), Array[Long], Array[String]] {

      /**
      * Method called when new data arrives.
      *
      * @param data
      * New data.
      * @param ps
      * Interface to ParameterServer.
      */
    override def onRecv(data: (String, Array[String]), ps: ParameterServerClient[Array[Long], Array[String]]): Unit = {

      val id = data._1.toLong
      val tweet = data._2

      val hashArray = (for (i <- 0 to math.ceil(numHashes / 64).toInt) yield LongHashFunction.xx(i).hashLong(id)).toArray


      for(word <- tweet) {
        ps.push(word.hashCode, hashArray)
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
                              paramValue: Array[Long],
                              ps: ParameterServerClient[Array[Long], Array[String]]): Unit = ???
    }

    val serverLogic = new BitSetBasedPSLogic(numHashes)


    val modelUpdates = FlinkParameterServer.transform(
      src,
      workerLogic,
      serverLogic,
      workerParallelism,
      psParallelism,
      iterationWaitTime)

    modelUpdates
      .flatMap(new RichFlatMapFunction[Either[Array[String], (Int, Vector)], (Int, Vector)] {
        override def flatMap(value: Either[Array[String], (Int, Vector)], out: Collector[(Int, Vector)]): Unit = {
          value match {
            case Left(_) =>
            case Right(param) => out.collect(param)
          }
        }
      })
  }
}
