package hu.sztaki.ilab.ps.sketch.bloom.filter

import java.lang.Math.floorMod

import hu.sztaki.ilab.ps.sketch.bloom.filter.pslogic.BloomPSLogic
import hu.sztaki.ilab.ps.{FlinkParameterServer, ParameterServerClient, WorkerLogic}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import hu.sztaki.ilab.ps.sketch.utils.Utils._

import scala.collection.mutable


/**
  *
  */

object BloomFilter {

  /**
    * Realise a Bloom filter (https://en.wikipedia.org/wiki/Bloom_filter) on top of a Parameter Server architecture.
    * In this solution the bloom filters are distributed horizontally on the server nodes.
    * @param src: Stream of datapoints, where the first element is the ID and the second is the value (e.g. Tweet)
    * @param arraySize: Parameter of the algorithm - m
    * @param numHashes: Parameter of the algorithm - k
    * @param workerParallelism: Number of worker nodes for the parameter server
    * @param psParallelism: Number of server nodes for the parameter server
    * @param iterationWaitTime: Parameter for Loop API
    * @return The learnt model
    */
  def bloomFilter(src: DataStream[(String, Array[String])],
                  arraySize: Int,
                  numHashes: Int,
                  workerParallelism: Int,
                  psParallelism: Int,
                  iterationWaitTime: Long) : DataStream[(Int,  mutable.BitSet)] = {

    val workerLogic = new WorkerLogic[(String, Array[String]), Vector, Array[String]] {

      /**
        * Method called when new data arrives.
        * For each tweet will be generated numHash number of hash values and will be pushed for each word in the tweet to the server nodes
        *
        * @param data
        * New data.
        * @param ps
        * Interface to ParameterServer.
        */
      override def onRecv(data: (String, Array[String]), ps: ParameterServerClient[Vector, Array[String]]): Unit = {

        val id = data._1
        val tweet = data._2

        val AS = (for(i <- 0 until numHashes)
          yield floorMod(scala.util.hashing.MurmurHash3.stringHash(id, i), arraySize))
          .toArray

        for(word <- tweet) {
          ps.push(word.hashCode, AS)
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
      override def onPullRecv(paramId: Int, paramValue: Vector, ps: ParameterServerClient[Vector, Array[String]]): Unit = ???
    }

    val serverLogic = new BloomPSLogic

    val modelUpdates = FlinkParameterServer.transform(
      src,
      workerLogic,
      serverLogic,
      workerParallelism,
      psParallelism,
      iterationWaitTime)


    modelUpdates
      .flatMap(new RichFlatMapFunction[Either[Array[String], (Int, mutable.BitSet)], (Int,  mutable.BitSet)] {
        override def flatMap(value: Either[Array[String], (Int,  mutable.BitSet)], out: Collector[(Int,  mutable.BitSet)]): Unit = {
          value match {
            case Left(_) =>
            case Right(param) => out.collect(param)
          }
        }
      })
  }
}