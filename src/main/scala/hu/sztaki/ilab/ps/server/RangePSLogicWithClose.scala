package hu.sztaki.ilab.ps.server

import hu.sztaki.ilab.ps.{ParameterServer, ParameterServerLogic}

class RangePSLogicWithClose[P](partitionsize: Int, paramInit: => Int => P, paramUpdate: => (P, P) => P) extends ParameterServerLogic[P, (Int, P)] {
  val params = new Array[Option[P]](partitionsize)

  @transient lazy val init: (Int) => P = paramInit
  @transient lazy val update: (P, P) => P = paramUpdate

  override def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[P, (Int, P)]): Unit =
    ps.answerPull(id, params(id).getOrElse({
      val initVal = init(id)
      params(id) = Some(initVal)
      initVal
    }), workerPartitionIndex)

  override def onPushRecv(id: Int, deltaUpdate: P, ps: ParameterServer[P, (Int, P)]): Unit = {
    val c = params(id) match {
      case Some(q) =>
        update(q, deltaUpdate)
      case None =>
        throw new IllegalStateException(
          "Parameter did not exist, was not able to update by any delta." +
            " You should not push before pulling!")
    }
    params(id) = Some(c)
  }

  /**
    * Method called when processing is finished.
    */
  override def close(ps: ParameterServer[P, (Int, P)]): Unit = {
    params.zipWithIndex.flatMap { case (pOpt, idx) => pOpt.map(p => (idx, p)) }
      .foreach { case (id, c) => ps.output(id, c) }
  }
}