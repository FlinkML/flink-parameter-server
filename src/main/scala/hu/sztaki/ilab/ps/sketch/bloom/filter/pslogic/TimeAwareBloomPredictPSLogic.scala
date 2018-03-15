package hu.sztaki.ilab.ps.sketch.bloom.filter.pslogic

import hu.sztaki.ilab.ps.{ParameterServer, ParameterServerLogic}
import hu.sztaki.ilab.ps.sketch.utils.Utils._

import scala.collection.mutable

/**
  * Server logic for predicting common occurrences
  * @param arraySize: Parameter of the algorithm - m
  * @param numHashes: Parameter of the algorithm - k
  * @param K: Size of the return list (top-K)
  */
class TimeAwareBloomPredictPSLogic(arraySize: Int, numHashes: Int, K: Int)
  extends ParameterServerLogic[Either[((Int, Int), Array[Int]),  (Int, Array[Int])], ((Int, Int), Array[(Double, Int)])] {

  val model = new mutable.HashMap[(Int, Int), Array[Int]]()

  override def onPullRecv(id: Int, workerPartitionIndex: Int,
                          ps: ParameterServer[Either[((Int, Int), Array[Int]),  (Int, Array[Int])],
                                              ((Int, Int), Array[(Double, Int)])]): Unit = {
    model
      .filter(_._1._1 == id)
      .foreach(p => {
        ps.answerPull(id, Left(p._1, p._2), workerPartitionIndex)
      })
  }


  override def onPushRecv(id: Int,
                          deltaUpdate: Either[((Int, Int), Array[Int]), (Int, Array[Int])],
                          ps: ParameterServer[Either[((Int, Int), Array[Int]),  (Int, Array[Int])],
                                              ((Int, Int), Array[(Double, Int)])]): Unit = {
    deltaUpdate match {
      case Left(((queryId, timeSlot), targetVector)) =>
        val topK = new mutable.ArrayBuffer[(Double, Int)]

        val nA = bloomEq(arraySize, numHashes, targetVector.length)
        val relevantVectors = model.filter(_._1._2 == timeSlot)

        for((k,v) <- relevantVectors){
          val nB = bloomEq(arraySize, numHashes, v.length)
          val union = bloomUnion(arraySize, numHashes, targetVector, v)
          val intersect = nA + nB - union
          topK += ((intersect, k._1))
        }

        ps.output(((queryId, timeSlot), topK.sorted.takeRight(K).toArray))

      case Right((timeSlot, newParam)) =>
        model.update((id, timeSlot), newParam)
    }
  }
}
