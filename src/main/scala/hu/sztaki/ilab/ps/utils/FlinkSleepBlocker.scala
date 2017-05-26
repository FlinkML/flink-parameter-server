package hu.sztaki.ilab.ps.utils

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._

object FlinkSleepBlocker {

  /**
    * Blocks a [[org.apache.flink.streaming.api.scala.DataStream]] for a given amount of time, before consuming it.
    *
    * Can be used to apply manual backpressure, e.g. when loading model into Parameter Server.
    *
    * @param stream
    * Input stream.
    * @param milliseconds
    * Milliseconds to block.
    * @tparam T
    * Type of input data.
    * @return
    * Blocked stream.
    */
  def block[T: TypeInformation](stream: DataStream[T], milliseconds: Long): DataStream[T] = {
    stream.forward.map(new RichMapFunction[T, T] {
      @transient lazy val sleeper: Unit = {
        Thread.sleep(milliseconds)
        ()
      }

      override def map(value: T): T = {
        sleeper
        value
      }

    }).setParallelism(stream.parallelism)
  }

}
