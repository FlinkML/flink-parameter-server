package hu.sztaki.ilab.ps.common

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration

class TimerLogic[W](intervalLength: FiniteDuration)
  extends Combinable[W]
    with Serializable {

  var timerThread: Option[Thread] = None
  var containsData = false

  def runTimer(callback: (Array[W] => Unit) => Unit, collect: Array[W] => Unit): Unit = {
    timerThread = Some(new Thread {
      override def run {
        while (true) {
          Thread sleep intervalLength.toMillis
          if (sendCondition()) {
            send(callback, collect)
            containsData = false
          }
        }
      }
    })
    timerThread.get.start()
  }

  /**
    * Should send when at least 1 message was received.
    */
  override def sendCondition(): Boolean = {
    containsData
  }

  /**
    * @param adder Adds the new message to a buffer
    * @param callback Calls back to the combined function or the collector to either check if collection is needed
    *                 or just collect
    * @param collectAnswerMsg Collector function
    */
  override def logic(adder: (ArrayBuffer[W]) => Unit,
                     callback: (Array[W] => Unit) => Unit,
                     collectAnswerMsg: Array[W] => Unit): Unit = {
    containsData = true

    if (!timerThread.isDefined) {
      runTimer(callback, collectAnswerMsg)
    }
  }

}
