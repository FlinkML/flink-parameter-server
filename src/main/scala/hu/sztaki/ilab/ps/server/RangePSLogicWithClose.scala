package hu.sztaki.ilab.ps.server

import hu.sztaki.ilab.ps.{ParameterServer, ParameterServerLogic}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.Configuration

class RangePSLogicWithClose[P](featureCount: Int, paramInit: => Int => P, paramUpdate: => (P, P) => P) extends ParameterServerLogic[Int, P, (Int, P)] {
  var startIndex = 0
  var params: Array[Option[P]] = _

  @transient lazy val init: (Int) => P = paramInit
  @transient lazy val update: (P, P) => P = paramUpdate

  override def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[Int, P, (Int, P)]): Unit = {
    if (id - startIndex < 0) {
      println(id)
      println(params.mkString("[", ",", "]"))
    }
    ps.answerPull(id, params(id - startIndex) match {
      case Some(e) => e
      case None => val ini = init(id)
        params(id - startIndex) = Some(ini)
        ini
    }, workerPartitionIndex)
  }


  override def onPushRecv(id: Int, deltaUpdate: P, ps: ParameterServer[Int, P, (Int, P)]): Unit = {
    val index = id  - startIndex
    val c = params(index) match {
      case Some(q) =>
        update(q, deltaUpdate)
      case None =>
        deltaUpdate
    }
    params(index) = Some(c)
  }

  /**
    * Method called when processing is finished.
    */
  override def close(ps: ParameterServer[Int, P, (Int, P)]): Unit =
    params.view.zipWithIndex.foreach{case(c: Option[P], id:Int) => c match {
      case Some(m) => ps.output((startIndex + id, m))
      case None => // Do nothing. The unused feature is not needed to write out.
    }}

  /**
    * Method called when the class is initialized.
    */
  override def open(parameters: Configuration, runtimeContext: RuntimeContext): Unit = {
    super.open(parameters, runtimeContext)
    val div = Math.ceil(featureCount.toDouble / runtimeContext.getNumberOfParallelSubtasks).toInt
    val mod = featureCount - (runtimeContext.getNumberOfParallelSubtasks - 1) * div
    params = Array.fill[Option[P]](
      if (mod != 0 && runtimeContext.getIndexOfThisSubtask + 1 == runtimeContext.getNumberOfParallelSubtasks) {
        mod
      } else {
        div
      })(None)
    startIndex = runtimeContext.getIndexOfThisSubtask * div
  }
}