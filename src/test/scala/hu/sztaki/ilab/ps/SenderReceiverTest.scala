package hu.sztaki.ilab.ps

import hu.sztaki.ilab.ps.client.sender._
import hu.sztaki.ilab.ps.common.Combinable
import hu.sztaki.ilab.ps.entities.{Pull, Push, WorkerToPS}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.scalatest.prop.PropertyChecks
import org.scalatest.time.{Seconds, Span}

import scala.collection.mutable
import scala.concurrent.duration._

class SenderReceiverTest extends FlatSpec with PropertyChecks with Matchers with BeforeAndAfter  {

  var pullMap: mutable.HashMap[Int, Pull] = mutable.HashMap()
  var pushMap: mutable.HashMap[Int, Push[Double]] = mutable.HashMap()

  def simpleWorkerAction(data: WorkerToPS[Double]): Unit = {
    data.msg match {
      case Left(pull) => pullMap += data.workerPartitionIndex -> pull
      case Right(push) => pushMap += data.workerPartitionIndex -> push
    }
  }

  def arrayWorkerAction(data: Array[WorkerToPS[Double]]): Unit = {
    data.foreach {
      out =>
        out.msg match {
          case Left(pull) => pullMap += out.workerPartitionIndex -> pull
          case Right(push) => pushMap += out.workerPartitionIndex -> push
        }
    }
  }

  before {
    pullMap.clear()
    pushMap.clear()
  }

  "simple client sender" should "work" in {
    val sender = new SimpleClientSender[Double]

    val pullRange = 1 to 3
    val pushRange = 4 to 10

    for (i <- pullRange) {
      sender.onPull(i, simpleWorkerAction, i)
    }
    for (i <- pushRange) {
      sender.onPush(i, i, simpleWorkerAction, i)
    }

    // Test is all the messages went through
    pullMap.size shouldEqual pullRange.size
    pushMap.size shouldEqual pushRange.size

    for (i <- pullRange) {
      assert(pullMap.get(i).isDefined)
    }

    for (i <- pushRange) {
      assert(pushMap.get(i).isDefined)
    }
  }

  "counter client sender" should "work" in {
    val sendAfter = 3

    val combinable: List[Combinable[WorkerToPS[Double]]] = List(CountClientSender(sendAfter))
    def condition(combinable: List[Combinable[WorkerToPS[Double]]]): Boolean = {
      combinable.head.shouldSend()
    }

    val sender = new CombinationClientSender[Double](condition, combinable)

    for (i <- 1 to sendAfter - 1) {
      sender.onPull(i, arrayWorkerAction, i)
    }

    // The limit was not hit, we receive nothing.
    pullMap.size shouldEqual 0
    pushMap.size shouldEqual 0

    sender.onPull(sendAfter, arrayWorkerAction, sendAfter)

    // After the limit was hit, we get all the messages
    pullMap.size shouldEqual sendAfter

    for (i <- 1 to sendAfter) {
      sender.onPush(i, i, arrayWorkerAction, i)
    }

    pushMap.size shouldEqual sendAfter

    sender.onPush(sendAfter + 1, sendAfter + 1, arrayWorkerAction, sendAfter + 1)

    pushMap.size shouldEqual sendAfter
  }

  "timer client sender" should "work" in {
    val combinable: List[Combinable[WorkerToPS[Double]]] = List(TimerClientSender(5 seconds))
    def condition(combinable: List[Combinable[WorkerToPS[Double]]]): Boolean = {
      combinable.head.shouldSend()
    }

    val sender = new CombinationClientSender[Double](condition, combinable)

    sender.onPull(1, arrayWorkerAction, 1)

    // The message won't arrive instantly
    pullMap.size shouldEqual 0

    // However it shall after the specified time has passed
    Eventually.eventually(Eventually.timeout(Span(10, Seconds)),
      Eventually.interval(Span(1, Seconds))) {
      pullMap.size shouldEqual 1
    }

    pullMap.clear()

    val range = 1 to 5

    for(i <- range) {
      sender.onPull(i, arrayWorkerAction, i)
      sender.onPush(i, i, arrayWorkerAction, i)
    }

    // Eventually we should get all the messages
    Eventually.eventually(Eventually.timeout(Span(10, Seconds)),
      Eventually.interval(Span(1, Seconds))) {
      pullMap.size shouldEqual range.end
      pushMap.size shouldEqual range.end
    }
  }

  "combined counter OR timer sender test" should "be really awesome" in {
    val countLimit = 5
    val timeLimit = 5 seconds
    val combinables: List[Combinable[WorkerToPS[Double]]] =
      List(CountClientSender(countLimit), TimerClientSender(timeLimit))

    // Meting either the counter or the timer condition should be enough. We are generous gods.
    def condition(combinables: List[Combinable[WorkerToPS[Double]]]): Boolean = {
      combinables.map(_.shouldSend()).reduce(_ || _)
    }

    val combinoSender = new CombinationClientSender[Double](condition, combinables)

    for (i <- 1 to countLimit - 1) {
      combinoSender.onPush(i, i, arrayWorkerAction, i)
    }

    // Before the limit is reached, there is nothing.
    pushMap.size shouldEqual 0

    combinoSender.onPush(countLimit, countLimit, arrayWorkerAction, countLimit)

    // When the limit is reached, the messages should be visible instantly.
    pushMap.size shouldEqual countLimit

    combinoSender.onPull(1, arrayWorkerAction, 1)

    // Only after a certain will the we get the messages.
    pullMap.size shouldEqual 0

    Eventually.eventually(Eventually.timeout(Span(10, Seconds)),
      Eventually.interval(Span(1, Seconds))) {
      pullMap.size shouldEqual 1
    }
  }

  "combined counter AND timer sender test" should "be really awesome" in {
    val countLimit = 5
    val timeLimit = 5 seconds
    val combinables: List[Combinable[WorkerToPS[Double]]] =
      List(CountClientSender(countLimit), TimerClientSender(timeLimit))

    // The counter and the timer condition should be met at the same time
    def condition(combinables: List[Combinable[WorkerToPS[Double]]]): Boolean = {
      combinables.map(_.shouldSend).reduce(_ && _)
    }

    val combinoSender = new CombinationClientSender[Double](condition, combinables)

    combinoSender.onPull(1, arrayWorkerAction, 1)

    // Sleep is not nice in tests, but how else can we wait to see if something does NOT change for a period?
    Thread.sleep(timeLimit.toMillis + 1000)

    pullMap.size shouldEqual 0

    for (i <- 1 to countLimit) {
      combinoSender.onPush(i, i, arrayWorkerAction, i)
    }

    Eventually.eventually(Eventually.timeout(Span(5, Seconds)),
      Eventually.interval(Span(1, Seconds))) {
      pullMap.size shouldEqual 1
      /* The count buffer contains countLimit + 1 messages now, the last one won't be sent until another countLimit - 1
         arrive.
       */
      pushMap.size shouldEqual countLimit - 1
    }
  }

}
