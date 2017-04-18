package hu.sztaki.ilab.ps.client.sender

import hu.sztaki.ilab.ps.ClientSender
import hu.sztaki.ilab.ps.entities._

class SimpleClientSender[P] extends ClientSender[WorkerToPS[P], P] {

  override def onPull(id: Int, collectAnswerMsg: WorkerToPS[P] => Unit, partitionId: Int): Unit = {
    collectAnswerMsg(WorkerToPS(partitionId, Left(Pull(id))))
  }

  override def onPush(id: Int, deltaUpdate: P, collectAnswerMsg: WorkerToPS[P] => Unit, partitionId: Int): Unit = {
    collectAnswerMsg(WorkerToPS(partitionId, Right(Push(id, deltaUpdate))))
  }
}
