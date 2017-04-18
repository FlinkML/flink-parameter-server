package hu.sztaki.ilab.ps.server.receiver

import hu.sztaki.ilab.ps.PSReceiver
import hu.sztaki.ilab.ps.entities.WorkerOut

class SimplePSReceiver[P] extends PSReceiver[WorkerOut[P], P] {

  override def onWorkerMsg(msg: WorkerOut[P],
                           onPullRecv: (Int, Int) => Unit,
                           onPushRecv: (Int, P) => Unit): Unit = {
    msg.msg match {
      case Left(l) =>
        // Passes key and partitionID
        onPullRecv(l, msg.partitionId)
      case Right(r) =>
        r match {
          case (key, delta) => onPushRecv(key, delta)
        }
      case _ =>
        throw new Exception("Parameter server received unknown message.")
    }
  }

}