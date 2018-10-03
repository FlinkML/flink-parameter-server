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
class BloomPredictPSLogic(arraySize: Int, numHashes: Int, K: Int)
  extends ParameterServerLogic[Int, Either[(Int, mutable.BitSet), mutable.BitSet], (Int, Array[(Double, Int)])] {

  val model = new mutable.HashMap[Int, mutable.BitSet]()

  /**
    * Just answer the requested vector
    * @param id
    * Identifier of parameter (e.g. it could be an index of a vector).
    * @param workerPartitionIndex
    * Index of the worker partition.
    * @param ps
    * Interface for answering pulls and creating output.
    */
  override def onPullRecv(id: Int, workerPartitionIndex: Int,
                          ps: ParameterServer[Int, Either[(Int, mutable.BitSet), mutable.BitSet],
                                                     (Int, Array[(Double, Int)])]): Unit = {
    ps.answerPull(id, Left((0, model.getOrElse(id, mutable.BitSet.empty))), workerPartitionIndex)
  }

  /**
    * Either updates the model in the first part (load the model) or calculate the local top-k for the received vector
    * @param id
    * Identifier of parameter (e.g. it could be an index of a vector).
    * @param deltaUpdate
    * Value to update the parameter (e.g. it could be added to the current value).
    * @param ps
    * Interface for answering pulls and creating output.
    */
  override def onPushRecv(id: Int, deltaUpdate: Either[(Int, mutable.BitSet), mutable.BitSet],
                          ps: ParameterServer[Int, Either[(Int, mutable.BitSet), mutable.BitSet], (Int, Array[(Double, Int)])]): Unit = {
    deltaUpdate match {

      case Left((queryId, targetVector)) =>
          val topK = new mutable.ArrayBuffer[(Double, Int)]

          val nA = bloomEq(arraySize, numHashes, targetVector.size)

          for((k,v) <- model){
            val nB = bloomEq(arraySize, numHashes, v.size)
            val union = bloomUnion(arraySize, numHashes, targetVector, v)
            val intersect = nA + nB - union
            topK += ((intersect, k))
          }
          ps.output((queryId, topK.sorted.takeRight(K).toArray))


      case Right(newParam) =>
        model.update(id, newParam)
    }
  }
}
