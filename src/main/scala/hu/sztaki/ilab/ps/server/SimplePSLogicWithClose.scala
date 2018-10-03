package hu.sztaki.ilab.ps.server

import hu.sztaki.ilab.ps.{ParameterServer, ParameterServerLogic}

import scala.collection.mutable

class SimplePSLogicWithClose[Id, P](paramInit: => Id => P, paramUpdate: => (P, P) => P) extends ParameterServerLogic[Id, P, (Id, P)] {
  val params = new mutable.HashMap[Id, P]()

  @transient lazy val init: (Id) => P = paramInit
  @transient lazy val update: (P, P) => P = paramUpdate

  override def onPullRecv(id: Id, workerPartitionIndex: Int, ps: ParameterServer[Id, P, (Id, P)]): Unit =
    ps.answerPull(id, params.getOrElseUpdate(id, init(id)), workerPartitionIndex)

  override def onPushRecv(id: Id, deltaUpdate: P, ps: ParameterServer[Id, P, (Id, P)]): Unit = {
    val c = params.get(id) match {
      case Some(q) =>
        update(q, deltaUpdate)
      case None =>
        deltaUpdate
    }
    params += ((id, c))
  }

  /**
    * Method called when processing is finished.
    */
  override def close(ps: ParameterServer[Id, P, (Id, P)]): Unit = {
    params.foreach{case(id, c) => ps.output(id, c)}
  }
}
