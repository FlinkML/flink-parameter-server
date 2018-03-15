package hu.sztaki.ilab.ps.sketch.bloom.filter

import java.lang.Math.floorMod

import hu.sztaki.ilab.ps.sketch.bloom.filter.pslogic.TimeAwareBloomPSLogic
import hu.sztaki.ilab.ps.{FlinkParameterServer, ParameterServerClient, WorkerLogic}
import hu.sztaki.ilab.ps.sketch.utils.Utils._
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable


/**
  * Modified version of bloom filter, where an additional timeslot parameter is added to each datapoint.
  * A different model will be built for each timeslot
  */
object TimeAwareBloomFilter {

  /**
    *
    * @param src: Stream of datapoints in the form of (ID, value, timeslot)
    * @param arraySize: Parameter of the algorithm - m
    * @param numHashes: Parameter of the algorithm - k
    * @param workerParallelism: Number of worker nodes for the parameter server
    * @param psParallelism: Number of server nodes for the parameter server
    * @param iterationWaitTime: Parameter for Loop API
    * @return The learnt model
    */
  def bloomFilter(src: DataStream[(String, Array[String], Int)],
                  arraySize: Int,
                  numHashes: Int,
                  workerParallelism: Int,
                  psParallelism: Int,
                  iterationWaitTime: Long): DataStream[((Int, Int), mutable.BitSet)] = {

    val workerLogic = new WorkerLogic[(String, Array[String], Int), (Int, Vector), Any] {
      /**
      * Method called when new data arrives.
      *
      * @param data
      * New data.
      * @param ps
      * Interface to ParameterServer.
      */
    override def onRecv(data: (String, Array[String], Int), ps: ParameterServerClient[(Int, Vector), Any]): Unit = {
      val AS = (for(i <- 0 until numHashes)
        yield floorMod(scala.util.hashing.MurmurHash3.stringHash(data._1, i), arraySize))
        .toArray

      for(word <- data._2){
        ps.push(word.hashCode,(data._3, AS))
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
      override def onPullRecv(paramId: Int, paramValue: (Int, Vector), ps: ParameterServerClient[(Int, Vector), Any]): Unit = ???
    }


    val serverLogic = new TimeAwareBloomPSLogic

    val modelUpdates = FlinkParameterServer.transform(
      src,
      workerLogic,
      serverLogic,
      workerParallelism,
      psParallelism,
      iterationWaitTime)

    modelUpdates
      .flatMap(
        new RichFlatMapFunction[Either[Any, ((Int, Int), mutable.BitSet)], ((Int, Int), mutable.BitSet)] {
        override def flatMap(value: Either[Any, ((Int, Int), mutable.BitSet)], out: Collector[( (Int, Int), mutable.BitSet)]): Unit = {
          value match {
            case Left(_) =>
            case Right(param) => out.collect(param)
          }
        }
      })
  }
}