package hu.sztaki.ilab.ps.server.sender

import hu.sztaki.ilab.ps.PSSender
import hu.sztaki.ilab.ps.entities.WorkerIn

class SimplePSSender[P] extends PSSender[WorkerIn[P], P]{

  override def onPullAnswer(id: Int, value: P, workerPartitionIndex: Int, collectAnswerMsg: (WorkerIn[P]) => Unit): Unit = {
    collectAnswerMsg(WorkerIn[P](id, workerPartitionIndex, value))
  }

}
