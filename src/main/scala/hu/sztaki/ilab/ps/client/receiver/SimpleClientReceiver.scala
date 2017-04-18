package hu.sztaki.ilab.ps.client.receiver

import hu.sztaki.ilab.ps.ClientReceiver
import hu.sztaki.ilab.ps.entities.{PSToWorker, PullAnswer}

class SimpleClientReceiver[P] extends ClientReceiver[PSToWorker[P], P] {

  override def onPullAnswerRecv(msg: PSToWorker[P], pullHandler: PullAnswer[P] => Unit): Unit =
    msg match {
      case PSToWorker(_, pullAns) => pullHandler(pullAns)
    }

}
