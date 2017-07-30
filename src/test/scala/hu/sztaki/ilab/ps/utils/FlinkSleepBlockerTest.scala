package hu.sztaki.ilab.ps.utils

import hu.sztaki.ilab.ps.test.utils.FlinkTestUtils._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.scalatest.{FlatSpec, Matchers}

object FlinkSleepBlockerTest {

  val blockMillis: Long = 5000

  val checkerSink = new SinkFunction[String] {

    var nonBlockedTime: Long = -1
    var blockedTime: Long = -1

    override def invoke(value: String): Unit = {
      value match {
        case "nonBlocked" =>
          nonBlockedTime = System.currentTimeMillis()
          // non-blocked should arrive before blocked
          assert(blockedTime == -1)
        case "blocked" =>
          blockedTime = System.currentTimeMillis()
          // non-blocked should arrive before blocked
          assert(nonBlockedTime != -1)
          throw new SuccessException[Long](blockedTime - nonBlockedTime)
      }
    }
  }
}

class FlinkSleepBlockerTest extends FlatSpec with Matchers {

  "sleep blocker" should "block for enough time" in {

    import FlinkSleepBlockerTest._

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val blockedSrc = FlinkSleepBlocker.block(
      env.fromElements("blocked").shuffle.map(x => x).setParallelism(2),
      blockMillis)

    val nonBlockedSrc =
      env.fromElements("nonBlocked").shuffle.map(x => x).setParallelism(3)

    blockedSrc.union(nonBlockedSrc).addSink(checkerSink).setParallelism(1)

    executeWithSuccessCheck[Long](env)(elapsedTime => {
      elapsedTime should be > (blockMillis * 0.9).toLong
      elapsedTime should be < (blockMillis * 1.5).toLong
    })
  }
}
