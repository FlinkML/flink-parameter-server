package hu.sztaki.ilab.ps.client.sender

import hu.sztaki.ilab.ps.WorkerSender
import hu.sztaki.ilab.ps.common.{Combinable, CombinationLogic}
import hu.sztaki.ilab.ps.entities._

import scala.collection.mutable.ArrayBuffer

class CombinationWorkerSender[Id, P](condition: (List[Combinable[WorkerToPS[Id, P]]]) => Boolean,
                                 combinables: List[Combinable[WorkerToPS[Id, P]]])
  extends CombinationLogic[WorkerToPS[Id, P]](condition, combinables)
    with WorkerSender[Array[WorkerToPS[Id, P]], Id, P]
    with Serializable {

  override def onPull(id: Id, collectAnswerMsg: Array[WorkerToPS[Id, P]] => Unit, partitionId: Int): Unit = {
    logic(
      (array: ArrayBuffer[WorkerToPS[Id, P]]) => {
        array += WorkerToPS(partitionId, Left(Pull(id)))
      },
      collectAnswerMsg
    )
  }

  override def onPush(id: Id,
                      deltaUpdate: P,
                      collectAnswerMsg: Array[WorkerToPS[Id, P]] => Unit,
                      partitionId: Int): Unit = {
    logic(
      (array: ArrayBuffer[WorkerToPS[Id, P]]) => {
        array += WorkerToPS(partitionId, Right(Push(id, deltaUpdate)))
      },
      collectAnswerMsg
    )
  }

}
