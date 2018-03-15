package hu.sztaki.ilab.ps.sketch.minhash.pslogic

import hu.sztaki.ilab.ps.{ParameterServer, ParameterServerLogic}
import net.openhft.hashing.LongHashFunction

import scala.collection.mutable

/**
  * Created by lukacsg on 2017.11.29..
  */
class StoredHashPSLogic(numHashes: Int) extends ParameterServerLogic[Long, (Int, Array[Long])] {

  val model = mutable.HashMap.empty[Int, Array[Long]]

  override def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[Long, (Int, Array[Long])]): Unit = ???

  override def onPushRecv(id: Int, tweetId: Long, ps: ParameterServer[Long, (Int, Array[Long])]): Unit =
    model.get(id) match {
      case Some(param) =>
        (0 until numHashes).foreach(i =>
          if (LongHashFunction.xx(i).hashLong(tweetId) < LongHashFunction.xx(i).hashLong(param(i)))
            param(i) = tweetId)
      case None =>
        model += ((id, Array.fill(numHashes)(id)))
    }

  override def close(ps: ParameterServer[Long, (Int, Array[Long])]): Unit =
    model.foreach{case(id, c) => ps.output(id, c)}
}
