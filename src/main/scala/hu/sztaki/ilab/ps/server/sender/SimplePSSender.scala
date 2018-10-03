package hu.sztaki.ilab.ps.server.sender

import hu.sztaki.ilab.ps.PSSender
import hu.sztaki.ilab.ps.entities.{PSToWorker, PullAnswer}

class SimplePSSender[Id, P] extends PSSender[PSToWorker[Id, P], Id, P]{

  override def onPullAnswer(id: Id, value: P, workerPartitionIndex: Int, collectAnswerMsg: (PSToWorker[Id, P]) => Unit): Unit = {
    collectAnswerMsg(PSToWorker[Id, P](workerPartitionIndex, PullAnswer(id, value)))
  }

}
