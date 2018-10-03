package hu.sztaki.ilab.ps.client.receiver

import hu.sztaki.ilab.ps.WorkerReceiver
import hu.sztaki.ilab.ps.entities.{PSToWorker, PullAnswer}

class MultipleWorkerReceiver[Id, P] extends WorkerReceiver[Array[PSToWorker[Id, P]], Id, P] {

  override def onPullAnswerRecv(msg: Array[PSToWorker[Id, P]], pullHandler: PullAnswer[Id, P] => Unit): Unit =
    msg.foreach {
      case PSToWorker(_, pullAns) => pullHandler(pullAns)
    }

}