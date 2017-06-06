package hu.sztaki.ilab.ps.utils

import org.apache.flink.api.common.functions.{Partitioner, RichFlatMapFunction}
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.scalatest.{FlatSpec, Matchers}
import FlinkEOF._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.util.Collector

import hu.sztaki.ilab.ps.test.utils.FlinkTestUtils._

import scala.util.matching.Regex

object FlinkEOFTest {

  val recordsPerSource = 5

  val srcFunc =
    new RichParallelSourceFunction[Int] {
      var cnt = 0

      override def cancel(): Unit = {
        cnt = recordsPerSource
      }

      override def run(ctx: SourceFunction.SourceContext[Int]): Unit = {
        val subtaskIdx = getRuntimeContext.getIndexOfThisSubtask

        // sleep time depends on the subtask id so that source subtasks finish at different times
        val sleepTime: Long = subtaskIdx * 100

        while (cnt < recordsPerSource) {
          Thread.sleep(sleepTime)
          ctx.collect(subtaskIdx)
          cnt += 1
        }
      }
    }

  val flatMapFunc = new RichFlatMapFunction[Int, String] with EOFHandler[String] {

    var isEOF = false
    var sum = 0

    override def flatMap(value: Int, out: Collector[String]): Unit = {
      if (isEOF) {
        throw new AssertionError("Should not have received input after EOF")
      } else {
        if (value % getRuntimeContext.getNumberOfParallelSubtasks != getRuntimeContext.getIndexOfThisSubtask) {
          throw new AssertionError("Unexpected record at subtask")
        }
        sum += value
      }
    }

    override def onEOF(collector: Collector[String]): Unit = {
      isEOF = true
      collector.collect(s"EOF $sum")
    }
  }

  val EOFSumPattern: Regex = "EOF (.*)".r

  val srcParallelism = 7
  val flatMapParallelism = 1

  val sinkFunc = new SinkFunction[String] {
    var sum: Int = 0
    var cnt: Int = 0

    override def invoke(value: String): Unit = value match {
      case EOFSumPattern(i) =>
        cnt += 1
        sum += i.toInt
        if (cnt == flatMapParallelism) {
          val expectedSum = (0 until srcParallelism).sum * recordsPerSource
          if (sum != expectedSum) {
            throw new AssertionError(s"Expected sum $expectedSum but got $sum.")
          } else {
            throw SuccessException[Int](sum)
          }
        } else if (cnt > flatMapParallelism) {
          throw new AssertionError(s"Should not have received more strings than $flatMapParallelism.")
        }
      case other =>
        throw new AssertionError(s"Unexpected string without EOF pattern: $other")
    }
  }
}

class FlinkEOFTest extends FlatSpec with Matchers {

  "eof" should "be able to mark end of input" in {

    import FlinkEOFTest._

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val src =
      env.addSource(srcFunc).setParallelism(srcParallelism)

    val mapped: DataStream[String] =
      FlinkEOF.flatMapWithEOF(src, flatMapFunc, flatMapParallelism, new Partitioner[Int] {
        override def partition(key: Int, numPartitions: Int): Int = {
          key % numPartitions
        }
      }, (x: Int) => x)

    mapped.addSink(sinkFunc).setParallelism(1)

    executeWithSuccessCheck[Int](env)(_ => ())
  }
}
