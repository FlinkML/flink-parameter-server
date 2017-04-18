package hu.sztaki.ilab.ps.server.sender

import hu.sztaki.ilab.ps.PSSender
import hu.sztaki.ilab.ps.common.{Combinable, CombinationLogic}
import hu.sztaki.ilab.ps.entities.WorkerIn

import scala.collection.mutable.ArrayBuffer

class CombinationPSSender[P](condition: (List[Combinable[WorkerIn[P]]]) => Boolean,
                             combinables: List[Combinable[WorkerIn[P]]])
  extends CombinationLogic[WorkerIn[P]](condition, combinables)
    with PSSender[Array[WorkerIn[P]], P]
    with Serializable {

  override def onPullAnswer(id: Int, value: P, workerPartitionIndex: Int, collectAnswerMsg: (Array[WorkerIn[P]]) => Unit): Unit = {
    logic(
      (array: ArrayBuffer[WorkerIn[P]]) => {
        array += WorkerIn(id, workerPartitionIndex, value)
      },
      collectAnswerMsg
    )
  }

}
