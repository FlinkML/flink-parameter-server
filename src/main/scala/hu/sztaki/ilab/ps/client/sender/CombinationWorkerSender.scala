package hu.sztaki.ilab.ps.client.sender

import hu.sztaki.ilab.ps.WorkerSender
import hu.sztaki.ilab.ps.common.{Combinable, CombinationLogic}
import hu.sztaki.ilab.ps.entities._

import scala.collection.mutable.ArrayBuffer

class CombinationWorkerSender[P](condition: (List[Combinable[WorkerToPS[P]]]) => Boolean,
                                 combinables: List[Combinable[WorkerToPS[P]]])
  extends CombinationLogic[WorkerToPS[P]](condition, combinables)
    with WorkerSender[Array[WorkerToPS[P]], P]
    with Serializable {

  override def onPull(id: Int, collectAnswerMsg: Array[WorkerToPS[P]] => Unit, partitionId: Int): Unit = {
    logic(
      (array: ArrayBuffer[WorkerToPS[P]]) => {
        array += WorkerToPS(partitionId, Left(Pull(id)))
      },
      collectAnswerMsg
    )
  }

  override def onPush(id: Int,
                      deltaUpdate: P,
                      collectAnswerMsg: Array[WorkerToPS[P]] => Unit,
                      partitionId: Int): Unit = {
    logic(
      (array: ArrayBuffer[WorkerToPS[P]]) => {
        array += WorkerToPS(partitionId, Right(Push(id, deltaUpdate)))
      },
      collectAnswerMsg
    )
  }

}
