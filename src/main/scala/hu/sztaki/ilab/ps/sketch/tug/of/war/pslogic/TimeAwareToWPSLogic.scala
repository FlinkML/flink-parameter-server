package hu.sztaki.ilab.ps.sketch.tug.of.war.pslogic

import hu.sztaki.ilab.ps.{ParameterServer, ParameterServerLogic}
import hu.sztaki.ilab.ps.sketch.utils.Utils.Vector

import scala.collection.mutable

class TimeAwareToWPSLogic(numHashes: Int) extends ParameterServerLogic[Int, (Int, Array[Long]), ((Int, Int), Vector)]{

  val model = new mutable.HashMap[(Int, Int), Vector]()

  override def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[Int, (Int, Array[Long]), ((Int, Int), Vector)]): Unit = ???


  override def onPushRecv(id: Int, deltaUpdate: (Int, Array[Long]), ps: ParameterServer[Int, (Int, Array[Long]), ((Int, Int), Vector)]): Unit = {
    val param = model.getOrElseUpdate((id, deltaUpdate._1), new Vector(numHashes))
    val update = collection.mutable.BitSet.fromBitMask(deltaUpdate._2)
    for(i <- 0 until numHashes){
      if(update(i)){
        param(i) += 1
      }
      else{
        param(i) -= 1
      }
    }
  }

  override def close(ps: ParameterServer[Int, (Int, Array[Long]), ((Int, Int), Vector)]): Unit = {
    model.foreach{case(id, c) => ps.output(id, c)}
  }
}
