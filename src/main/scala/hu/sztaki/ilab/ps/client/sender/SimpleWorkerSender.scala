package hu.sztaki.ilab.ps.client.sender

import hu.sztaki.ilab.ps.WorkerSender
import hu.sztaki.ilab.ps.entities._

class SimpleWorkerSender[Id, P] extends WorkerSender[WorkerToPS[Id, P], Id, P] {

  override def onPull(id: Id, collectAnswerMsg: WorkerToPS[Id, P] => Unit, partitionId: Int): Unit = {
    collectAnswerMsg(WorkerToPS(partitionId, Left(Pull(id))))
  }

  override def onPush(id: Id, deltaUpdate: P, collectAnswerMsg: WorkerToPS[Id, P] => Unit, partitionId: Int): Unit = {
    collectAnswerMsg(WorkerToPS(partitionId, Right(Push(id, deltaUpdate))))
  }
}
