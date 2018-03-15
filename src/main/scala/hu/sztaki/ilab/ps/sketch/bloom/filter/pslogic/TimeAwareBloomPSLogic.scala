package hu.sztaki.ilab.ps.sketch.bloom.filter.pslogic

import hu.sztaki.ilab.ps.{ParameterServer, ParameterServerLogic}
import hu.sztaki.ilab.ps.sketch.utils.Utils.Vector

import scala.collection.mutable

/**
  * Server logic for storing and updating bloom filters for each timeslot
  */
class TimeAwareBloomPSLogic  extends ParameterServerLogic[(Int, Vector), ((Int, Int), mutable.BitSet)]{

  val model = new mutable.HashMap[(Int, Int), mutable.ArrayBuffer[Int]]()

  override def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[(Int, Vector), ((Int, Int), mutable.BitSet)]): Unit = ???

  override def onPushRecv(id: Int, deltaUpdate: (Int, Vector), ps: ParameterServer[(Int, Vector), ((Int, Int), mutable.BitSet)]): Unit = {
    val param = model.getOrElseUpdate((id, deltaUpdate._1), new mutable.ArrayBuffer[Int]())
    for(elem <- deltaUpdate._2){
      param += elem
    }
  }


  override def close(ps: ParameterServer[(Int, Vector), ((Int, Int), mutable.BitSet)]): Unit = {
    model.foreach{case(id, c) =>
      val as = mutable.BitSet.empty
      c.foreach(as += _)
      ps.output(id, as)}
  }
}
