package hu.sztaki.ilab.ps.common

import scala.collection.mutable.ArrayBuffer

class CountLogic[W](max: Int) extends Combinable[W] with Serializable {

  var count = 0

  override def sendCondition(): Boolean = {
    count >= max
  }

  /*
  @todo Separately count pulls and pushes if need be?
            -> Since the logic differs heavily this might need a separate implementation.
   */
  override def logic(adder: (ArrayBuffer[W]) => Unit,
                     callback: (Array[W] => Unit) => Unit,
                     collectAnswerMsg: Array[W] => Unit): Unit = {
    count += 1

    if (sendCondition()) {
      send(callback, collectAnswerMsg)

      count = 0
    }
  }

}
