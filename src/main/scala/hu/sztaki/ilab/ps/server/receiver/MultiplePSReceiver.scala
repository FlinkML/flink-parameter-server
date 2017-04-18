package hu.sztaki.ilab.ps.server.receiver

import hu.sztaki.ilab.ps.PSReceiver
import hu.sztaki.ilab.ps.entities.WorkerOut

class MultiplePSReceiver[P] extends PSReceiver[Array[WorkerOut[P]], P] {

  override def onWorkerMsg(msg: Array[WorkerOut[P]],
                           onPullRecv: (Int, Int) => Unit,
                           onPushRecv: (Int, P) => Unit): Unit = {
    // @todo might not be optimal?
    // @todo duplicate code from the simple version
    msg.foreach(
      out =>
        out.msg match {
          case Left(l) =>
            // Passes key and partitionID
            onPullRecv(l, out.partitionId)
          case Right(r) =>
            r match {
              case (key, delta) => onPushRecv(key, delta)
            }
          case _ =>
            throw new Exception("Parameter server received unknown message.")
        }
    )
  }

}