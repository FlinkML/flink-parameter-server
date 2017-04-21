package hu.sztaki.ilab.ps.utils

import org.apache.flink.api.common.functions.{Partitioner, RichFlatMapFunction, RuntimeContext}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FlinkEOF {

  /**
    * Function to handle end of input (end of file, EOF) event. This should be used in combination with a
    * [[RichFlatMapFunction]].
    *
    * @tparam OUT
    * Type of output.
    */
  trait EOFHandler[OUT] {
    def onEOF(collector: Collector[OUT])
  }

  private case class EOF(sourceSubtask: Int, targetSubtask: Int) extends Serializable

  private class RichFlatMapWrapper[IN, OUT](inParallelism: Int, eofHandler: RichFlatMapFunction[IN, OUT] with
    EOFHandler[OUT])
    extends RichFlatMapFunction[Either[EOF, IN], OUT] {

    var eofCount = 0

    override def flatMap(value: Either[EOF, IN], out: Collector[OUT]): Unit = {
      value match {
        case Left(EOF(sourceSubtask, _)) =>
          eofCount += 1
          if (eofCount >= inParallelism) {
            eofHandler.onEOF(out)
          }
        case Right(x) =>
          eofHandler.flatMap(x, out)
      }
    }

    override def open(parameters: Configuration): Unit = {
      eofHandler.open(parameters)
    }

    override def setRuntimeContext(t: RuntimeContext): Unit = {
      eofHandler.setRuntimeContext(t)
    }

    override def close(): Unit = {
      eofHandler.close()
    }
  }

  /**
    * Adds an end of input (end of file, EOF) event handler to a [[RichFlatMapFunction]].
    *
    * @param in
    * Input [[DataStream]].
    * @param flatMapFunction
    * User defined [[RichFlatMapFunction]] extended with an [[EOFHandler]].
    * @param downstreamParallelism
    * Parallelism of the [[RichFlatMapFunction]] operator.
    * @param partitioner
    * Partitioner of the input to the user defined [[RichFlatMapFunction]].
    * @param partitionerFunc
    * Function mapping input ([[IN]]) to partitioning key [[K]].
    * @return
    * Output [[DataStream]] of the applied user defined [[RichFlatMapFunction]].
    */
  def flatMapWithEOF[IN, OUT, K,
  FlatMapFunc <: RichFlatMapFunction[IN, OUT]
    with EOFHandler[OUT]](in: DataStream[IN],
                          flatMapFunction: FlatMapFunc,
                          // todo use broadcast to avoid defining explicit parallelism and partitioner
                          downstreamParallelism: Int,
                          partitioner: Partitioner[K],
                          partitionerFunc: IN => K)
                         (implicit inTI: TypeInformation[IN],
                          outTI: TypeInformation[OUT],
                          kTI: TypeInformation[K]): DataStream[OUT] = {

    val keyExtractor: Either[EOF, IN] => Either[EOF, K] = {
      case Left(eof) => Left(eof)
      case Right(value) => Right(partitionerFunc(value))
    }

    in.forward
      .flatMap(new RichFlatMapFunction[IN, Either[EOF, IN]] {
        var collector: Option[Collector[Either[EOF, IN]]] = None

        override def flatMap(value: IN, out: Collector[Either[EOF, IN]]): Unit = {
          collector = Some(out)
          out.collect(Right(value))
        }

        override def close(): Unit = {
          collector match {
            case Some(out) =>
              for (targetSubtask <- 0 until downstreamParallelism) {
                out.collect(Left(EOF(getRuntimeContext.getIndexOfThisSubtask, targetSubtask)))
              }
            case None =>
              // fixme support empty source by sending a trigger element in the source
              throw new UnsupportedOperationException("Empty source instance, cannot mark EOF.")
          }
        }
      })
      .setParallelism(in.parallelism)
      .partitionCustom(new Partitioner[Either[EOF, K]] {
        override def partition(key: Either[EOF, K], numPartitions: Int): Int = {
          key match {
            case Left(EOF(_, targetSubtask)) =>
              targetSubtask
            case Right(x) => partitioner.partition(x, numPartitions)
          }
        }
      }, keyExtractor)
      .flatMap(new RichFlatMapWrapper[IN, OUT](in.parallelism, flatMapFunction))
      .setParallelism(downstreamParallelism)
      .forward
  }

}
