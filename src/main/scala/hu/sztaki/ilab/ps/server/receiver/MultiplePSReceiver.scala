package hu.sztaki.ilab.ps.server.receiver

import hu.sztaki.ilab.ps.PSReceiver
import hu.sztaki.ilab.ps.entities.{Pull, Push, WorkerToPS}

class MultiplePSReceiver[Id, P] extends PSReceiver[Array[WorkerToPS[Id, P]], Id, P] {

  override def onWorkerMsg(msg: Array[WorkerToPS[Id, P]],
                           onPullRecv: (Id, Int) => Unit,
                           onPushRecv: (Id, P) => Unit): Unit = {
    msg.foreach {
      wToPS =>
        val workerPartition = wToPS.workerPartitionIndex
        wToPS.msg match {
          case Left(Pull(paramId)) =>
            onPullRecv(paramId, workerPartition)
          case Right(Push(paramId, delta)) =>
            onPushRecv(paramId, delta)
          case _ =>
            throw new Exception("Parameter server received unknown message.")
        }
    }

  }

}