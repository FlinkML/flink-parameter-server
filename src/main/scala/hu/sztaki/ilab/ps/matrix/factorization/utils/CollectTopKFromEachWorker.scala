package hu.sztaki.ilab.ps.matrix.factorization.utils


import hu.sztaki.ilab.ps.matrix.factorization.utils.Utils.{ItemId, TopKQueue, TopKWorkerOutput, UserId}
import hu.sztaki.ilab.ps.matrix.factorization.utils.Vector._
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * A flat map function that receives TopKs from each worker, and outputs the overall TopK.
  * Used on data coming out of the parameter server.
  *
  * The Input contains a disjoint union of TopK worker outputs on the Left, and
  * Server outputs on the Right (which will be discarded)
  *
  * The Output contains tuples of user ID, item ID, timestamp, and TopK List
  * @param K Number of items in the generated recommendation
  * @param memory The last #memory item seen by the user will not be recommended
  * @param workerParallelism Number of worker nodes
  */
class CollectTopKFromEachWorker(K: Int, memory: Int, workerParallelism: Int)
  extends RichFlatMapFunction[Either[TopKWorkerOutput, (UserId, LengthAndVector)], (UserId, ItemId, Long, List[(Double, ItemId)])] {

  val outputs = new mutable.HashMap[Double, Array[TopKQueue]]
  val seenSet = new mutable.HashMap[UserId, mutable.HashSet[ItemId]]
  val seenList = new mutable.HashMap[UserId, mutable.Queue[ItemId]]

  override def flatMap(value: Either[TopKWorkerOutput, (UserId, (VectorLength, Vector))],
                       out: Collector[(UserId, ItemId, Long, List[(Double, ItemId)])]): Unit = {

    value match {
      case Left((RichRating(userId, itemId, _, workerId, ratingId, timestamp), actualTopK)) =>

        val allTopK = outputs.getOrElseUpdate(ratingId, new Array[TopKQueue](workerParallelism))

        allTopK(workerId) = actualTopK

        if (allTopKReceived(allTopK)) {
          val topKQueue = allTopK.fold(new mutable.ListBuffer[(Double, ItemId)]())((q1, q2) => q1 ++= q2)
          val topKList = topKQueue.toList
            .filterNot(x => seenSet.getOrElseUpdate(userId, new mutable.HashSet) contains x._2)
            .sortBy(-_._1)
            .take(K)

          out.collect((userId, itemId, timestamp, topKList))
          outputs -= ratingId

          seenSet.getOrElseUpdate(userId, new mutable.HashSet) += itemId
          seenList.getOrElseUpdate(userId, new mutable.Queue) += itemId
          if ((memory > -1) && (seenList(userId).length > memory)) {
            seenSet(userId) -= seenList(userId).dequeue()
          }
        }

      case Right(_) =>
    }
  }

  def allTopKReceived(allTopK: Array[TopKQueue]): Boolean = {
    var numReceivedTopKs = 0

    for (topK <- allTopK) {
      if (topK != null) {
        numReceivedTopKs += 1
      }
    }

    numReceivedTopKs == workerParallelism
  }


}
