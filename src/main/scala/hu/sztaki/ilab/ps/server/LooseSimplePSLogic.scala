package hu.sztaki.ilab.ps.server

import hu.sztaki.ilab.ps.{LooseParameterServerLogic, ParameterServer}

import scala.collection.mutable

class LooseSimplePSLogic[PullP, PushP](paramInit: => Int => PullP, paramUpdate: => (PullP, PushP) => PullP)
  extends LooseParameterServerLogic[PullP, PushP, (Int, PullP)] {
  val params = new mutable.HashMap[Integer, PullP]()

  @transient lazy val init: (Int) => PullP = paramInit
  @transient lazy val update: (PullP, PushP) => PullP = paramUpdate

  override def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[PullP, (Int, PullP)]): Unit =
    ps.answerPull(id, params.getOrElseUpdate(id, init(id)), workerPartitionIndex)

  override def onPushRecv(id: Int, deltaUpdate: PushP, ps: ParameterServer[PullP, (Int, PullP)]): Unit = {
    val c = params.get(id) match {
      case Some(q) =>
        update(q, deltaUpdate)
      case None =>
//        deltaUpdate
        init(id)
    }
    params += ((id, c))
    ps.output((id, c))
  }
}
