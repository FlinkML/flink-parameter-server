package hu.sztaki.ilab.ps.sketch.minhash.pslogic

import hu.sztaki.ilab.ps.{ParameterServer, ParameterServerLogic}
import hu.sztaki.ilab.ps.sketch.utils.Utils.Vector

import scala.collection.mutable

/**
  * Created by lukacsg on 2017.11.29..
  */
class SendHashPSLogic(numHashes: Int) extends ParameterServerLogic[Int, (Long, Vector), (Int, Array[Long])] {

  val model = mutable.HashMap.empty[Int, Array[(Long, Int)]]

  override def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[Int, (Long, Vector), (Int, Array[Long])]): Unit = ???

  override def onPushRecv(id: Int, deltaUpdate: (Long, Vector), ps: ParameterServer[Int, (Long, Vector), (Int, Array[Long])]): Unit = {
    val (tweetId, hashValues) = deltaUpdate
    var changed = false
    val update = model.get(id) match {
      case Some(param) =>
        param
          .view
          .zipWithIndex
          .map{
            case((storedTweetId, storedHashValue), index) =>
              val newHashValue = hashValues(index)
              if(storedHashValue <= newHashValue)
                (storedTweetId, storedHashValue)
              else {
                changed = true
                (tweetId, newHashValue)
              }
        }.toArray
      case None =>
        changed = true
        hashValues.map((tweetId, _))
    }
    if(changed) model += ((id, update))
  }

  override def close(ps: ParameterServer[Int, (Long, Vector), (Int, Array[Long])]): Unit =
    model.foreach{case(id, c) => ps.output(id, c.map(_._1))}
}
