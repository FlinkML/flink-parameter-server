package hu.sztaki.ilab.ps.sketch.tug.of.war.pslogic

import hu.sztaki.ilab.ps.sketch.utils.Utils._
import hu.sztaki.ilab.ps.{ParameterServer, ParameterServerLogic}

import scala.collection.mutable

class BitSetBasedPSLogic(numHashes: Int) extends ParameterServerLogic[Array[Long], (Int, Vector)]{

  val model = new mutable.HashMap[Int, Vector]()

  override def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[Array[Long], (Int, Vector)]): Unit = ???

  override def onPushRecv(id: Int, deltaUpdate: Array[Long], ps: ParameterServer[Array[Long], (Int, Vector)]): Unit = {
    val param = model.getOrElseUpdate(id, new Vector(numHashes))
    val update = collection.mutable.BitSet.fromBitMask(deltaUpdate)
    for(i <- 0 until numHashes){
      if(update(i)){
        param(i) += 1
      }
      else{
        param(i) -= 1
      }
    }
  }

  override def close(ps: ParameterServer[Array[Long], (Int, Vector)]): Unit = {
    model.foreach{case(id, c) => ps.output(id, c)}
  }
}
