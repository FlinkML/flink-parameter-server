package hu.sztaki.ilab.ps.client.receiver

import hu.sztaki.ilab.ps.WorkerReceiver
import hu.sztaki.ilab.ps.entities.{PSToWorker, PullAnswer}

class SimpleWorkerReceiver[Id, P] extends WorkerReceiver[PSToWorker[Id, P], Id, P] {

  override def onPullAnswerRecv(msg: PSToWorker[Id, P], pullHandler: PullAnswer[Id, P] => Unit): Unit =
    msg match {
      case PSToWorker(_, pullAns) => pullHandler(pullAns)
    }

}
