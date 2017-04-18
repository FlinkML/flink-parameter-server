package hu.sztaki.ilab.ps.client.receiver

import hu.sztaki.ilab.ps.ClientReceiver
import hu.sztaki.ilab.ps.entities.{PSToWorker, PullAnswer}

class MultipleClientReceiver[P] extends ClientReceiver[Array[PSToWorker[P]], P] {

  override def onPullAnswerRecv(msg: Array[PSToWorker[P]], pullHandler: PullAnswer[P] => Unit): Unit =
    msg.foreach {
      case PSToWorker(_, pullAns) => pullHandler(pullAns)
    }

}