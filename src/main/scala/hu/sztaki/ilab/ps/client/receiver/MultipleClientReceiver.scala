package hu.sztaki.ilab.ps.client.receiver

import hu.sztaki.ilab.ps.ClientReceiver
import hu.sztaki.ilab.ps.entities.WorkerIn

class MultipleClientReceiver[P] extends ClientReceiver[Array[WorkerIn[P]], P] {

  override def onPullAnswerRecv(msg: Array[WorkerIn[P]], pullHandler: (Int, P) => Unit): Unit = {
    // @todo might not be optimal?
    msg.foreach(
      in => pullHandler(in.id, in.msg)
    )
  }

}