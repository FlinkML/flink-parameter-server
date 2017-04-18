package hu.sztaki.ilab.ps.client.sender

import hu.sztaki.ilab.ps.ClientSender
import hu.sztaki.ilab.ps.entities.WorkerOut

class SimpleClientSender[P] extends ClientSender[WorkerOut[P], P] {

  override def onPull(id: Int, collectAnswerMsg: WorkerOut[P] => Unit, partitionId: Int): Unit = {
    collectAnswerMsg(WorkerOut(partitionId, Left(id)))
  }

  override def onPush(id: Int, deltaUpdate: P, collectAnswerMsg: WorkerOut[P] => Unit, partitionId: Int): Unit = {
    collectAnswerMsg(WorkerOut(partitionId, Right(id, deltaUpdate)))
  }
}
