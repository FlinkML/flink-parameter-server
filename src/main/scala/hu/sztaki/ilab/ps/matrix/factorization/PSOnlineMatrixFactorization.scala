package hu.sztaki.ilab.ps.matrix.factorization

import hu.sztaki.ilab.ps.matrix.factorization.factors.{RangedRandomFactorInitializerDescriptor, SGDUpdater}
import hu.sztaki.ilab.ps.matrix.factorization.utils.Rating
import hu.sztaki.ilab.ps.matrix.factorization.utils.Utils.{ItemId, UserId}
import hu.sztaki.ilab.ps.matrix.factorization.utils.Vector._
import hu.sztaki.ilab.ps.server.SimplePSLogic
import hu.sztaki.ilab.ps.{FlinkParameterServer, ParameterServerClient, WorkerLogic}
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable
import scala.util.Random

class PSOnlineMatrixFactorization {
}

/**
  * A use-case for PS: online matrix factorization with SGD
  * At every Rating in the DataStream, the worker, where the user vector is stored, pulls the relevant
  * item vector, applies the SGD updates and pushes the updates
  */

object PSOnlineMatrixFactorization{

  /**
    *
    * @param src A flink data stream containing [[utils.Rating]]s
    * @param numFactors Number of latent factors
    * @param rangeMin Lower bound of the random number generator
    * @param rangeMax Upper bound of the random number generator
    * @param learningRate Learning rate of SGD
    * @param negativeSampleRate Number of negative samples (Ratings with rate = 0) for each positive rating
    * @param userMemory The last #memory item seen by the user will not be generated as negative sample
    * @param pullLimit  Upper limit of unanswered pull requests in the system
    * @param workerParallelism Number of workernodes
    * @param psParallelism Number of parameter server nodes
    * @param iterationWaitTime Time without new rating before shutting down the system (never stops if set to 0)
    * @return For each rating the updated (userId, userVectors) / (itemId, Vectors) tuples
    */
  def psOnlineMF(src: DataStream[Rating],
                 numFactors: Int = 10,
                 rangeMin: Double = -0.01,
                 rangeMax: Double = 0.01,
                 learningRate: Double,
                 negativeSampleRate: Int = 0,
                 userMemory: Int = 128,
                 pullLimit: Int = 1600,
                 workerParallelism: Int,
                 psParallelism: Int,
                 iterationWaitTime: Long = 10000): DataStream[Either[(UserId, Vector), (ItemId, Vector)]] = {

    // initialization method and update method
    val factorInitDesc = RangedRandomFactorInitializerDescriptor(numFactors, rangeMin, rangeMax)
    val factorUpdate = new SGDUpdater(learningRate)

    val workerLogicBase = new WorkerLogic[Rating, Vector, (UserId, Vector)] {

      val userVectors = new mutable.HashMap[UserId, Vector]
      val ratingBuffer = new mutable.HashMap[ItemId, mutable.Queue[Rating]]()
      val itemIds = new mutable.ArrayBuffer[ItemId]
      val seenItemsSet = new mutable.HashMap[UserId, mutable.HashSet[ItemId]]
      val seenItemsQueue = new mutable.HashMap[UserId, mutable.Queue[ItemId]]

      override
      def onPullRecv(paramId: ItemId, paramValue: Vector, ps: ParameterServerClient[Vector, (UserId, Vector)]): Unit = {
        val rating = ratingBuffer synchronized {
          ratingBuffer(paramId).dequeue()
        }

        val user = userVectors.getOrElseUpdate(rating.user, factorInitDesc.open().nextFactor(rating.user))
        val item = paramValue
        val (userDelta, itemDelta) = factorUpdate.delta(rating.rating, user, item)

        userVectors(rating.user) = vectorSum(user, userDelta)

        ps.output(rating.user, userVectors(rating.user))
        ps.push(paramId, itemDelta)
      }


      override
      def onRecv(data: Rating, ps: ParameterServerClient[Vector, (UserId, Vector)]): Unit = {

        val seenSet = seenItemsSet.getOrElseUpdate(data.user, new mutable.HashSet)
        val seenQueue = seenItemsQueue.getOrElseUpdate(data.user, new mutable.Queue)

        if (seenQueue.length >= userMemory) {
          seenSet -= seenQueue.dequeue()
        }
        seenSet += data.item
        seenQueue += data.item

        ratingBuffer synchronized {
          for(_  <- 1 to Math.min(itemIds.length - seenSet.size, negativeSampleRate)){
            var randomItemId = itemIds(Random.nextInt(itemIds.size))
            while (seenSet contains randomItemId) {
              randomItemId = itemIds(Random.nextInt(itemIds.size))
            }
            ratingBuffer(randomItemId).enqueue(Rating(data.user, randomItemId, 0.0, data.timestamp))
            ps.pull(randomItemId)
          }

          ratingBuffer.getOrElseUpdate(
            data.item,
            {
              itemIds += data.item
              mutable.Queue[Rating]()
            }).enqueue(data)
        }

        ps.pull(data.item)
      }
    }

    val workerLogic: WorkerLogic[Rating, Vector, (UserId, Vector)] =
      WorkerLogic.addBlockingPullLimiter(workerLogicBase, pullLimit)

    val serverLogic = new SimplePSLogic[Array[Double]](
      x => factorInitDesc.open().nextFactor(x), { (vec, deltaVec) => vectorSum(vec, deltaVec)}
    )

    val partitionedInput = src.partitionCustom(new Partitioner[Int] {
      override def partition(key: UserId, numPartitions: Int): ItemId = { key % numPartitions }
    }, x => x.user)

    val modelUpdates = FlinkParameterServer.transform(
      partitionedInput,
      workerLogic,
      serverLogic,
      workerParallelism,
      psParallelism,
      iterationWaitTime)

    modelUpdates
  }
}
