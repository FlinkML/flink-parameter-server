package hu.sztaki.ilab.ps.common

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class CombinationLogic[W](condition: (List[Combinable[W]]) => Boolean,
                             combinables: List[Combinable[W]])
  extends Serializable {

  var data: ArrayBuffer[W] = ArrayBuffer()

  def checkAndSend(collect: Array[W] => Unit)(implicit ev: ClassTag[W]): Unit = {
    // Check if the combined condition passes
    if (condition(combinables)) {
      // Send the messages
      collect(data.toArray[W])
      // Empty the buffer, we're starting over
      data.clear()
      // Signal to the client handlers that the messages have been sent.
      combinables.foreach(
        _.reset()
      )
    }
  }

  def logic(func: ArrayBuffer[W] => Unit,
            collect: Array[W] => Unit)(implicit ev: ClassTag[W]): Unit = {
    func(data)

    combinables.foreach(
      combinable => combinable.logic(func, checkAndSend, collect)
    )
  }

}
