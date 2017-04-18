package hu.sztaki.ilab.ps.client.receiver

import hu.sztaki.ilab.ps.ClientReceiver
import hu.sztaki.ilab.ps.entities.WorkerIn

class SimpleClientReceiver[P] extends ClientReceiver[WorkerIn[P], P] {

  override def onPullAnswerRecv(msg: WorkerIn[P], pullHandler: (Int, P) => Unit): Unit = {
    pullHandler(msg.id, msg.msg)
  }

}
