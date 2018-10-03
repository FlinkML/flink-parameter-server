package hu.sztaki.ilab.ps.server

import hu.sztaki.ilab.ps.{LooseParameterServerLogic, ParameterServer}

import scala.collection.mutable

class LooseSimplePSLogicWithClose[Id, PullP, PushP](paramInit: => Id => PullP, paramUpdate: => (PullP, PushP) => PullP,
                                                store: PushP => PullP)
  extends LooseParameterServerLogic[Id, PullP, PushP, (Id, PullP)] {
  val params = new mutable.HashMap[Id, PullP]()

  @transient lazy val init: (Id) => PullP = paramInit
  @transient lazy val update: (PullP, PushP) => PullP = paramUpdate

  override def onPullRecv(id: Id, workerPartitionIndex: Int, ps: ParameterServer[Id, PullP, (Id, PullP)]): Unit =
    ps.answerPull(id, params.getOrElseUpdate(id, init(id)), workerPartitionIndex)

  override def onPushRecv(id: Id, deltaUpdate: PushP, ps: ParameterServer[Id, PullP, (Id, PullP)]): Unit = {
    val c = params.get(id) match {
      case Some(q) =>
        update(q, deltaUpdate)
      case None =>
        store(deltaUpdate)
    }
    params += ((id, c))
  }

  /**
    * Method called when processing is finished.
    */
  override def close(ps: ParameterServer[Id, PullP, (Id, PullP)]): Unit = {
    params.foreach{case(id, c) => ps.output(id, c)}
  }
}
