package hu.sztaki.ilab.ps.server

import hu.sztaki.ilab.ps.{LooseParameterServerLogic, ParameterServer}

import scala.collection.mutable

class LooseSimplePSLogicWithClose[PullP, PushP](paramInit: => Int => PullP, paramUpdate: => (PullP, PushP) => PullP,
                                                store: PushP => PullP)
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
        store(deltaUpdate)
    }
    params += ((id, c))
  }

  /**
    * Method called when processing is finished.
    */
  override def close(ps: ParameterServer[PullP, (Int, PullP)]): Unit = {
    params.foreach{case(id, c) => ps.output(id, c)}
  }
}
