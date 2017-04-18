package hu.sztaki.ilab.ps.client.sender

import hu.sztaki.ilab.ps.ClientSender
import hu.sztaki.ilab.ps.common.{Combinable, CombinationLogic}
import hu.sztaki.ilab.ps.entities.WorkerOut

import scala.collection.mutable.ArrayBuffer

class CombinationClientSender[P](
         condition: (List[Combinable[WorkerOut[P]]]) => Boolean,
         combinables: List[Combinable[WorkerOut[P]]])
  extends CombinationLogic[WorkerOut[P]](condition, combinables)
    with ClientSender[Array[WorkerOut[P]], P]
    with Serializable {

  override def onPull(id: Int, collectAnswerMsg: Array[WorkerOut[P]] => Unit, partitionId: Int): Unit = {
    logic(
      (array: ArrayBuffer[WorkerOut[P]]) => {
        array += WorkerOut(partitionId, Left(id))
      },
      collectAnswerMsg
    )
  }

  override def onPush(id: Int,
                      deltaUpdate: P,
                      collectAnswerMsg: Array[WorkerOut[P]] => Unit,
                      partitionId: Int): Unit = {
    logic(
      (array: ArrayBuffer[WorkerOut[P]]) => {
        array += WorkerOut(partitionId, Right(id, deltaUpdate))
      },
      collectAnswerMsg
    )
  }

}
