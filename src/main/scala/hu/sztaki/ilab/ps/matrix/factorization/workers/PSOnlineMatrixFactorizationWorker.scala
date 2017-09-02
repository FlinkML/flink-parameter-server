package hu.sztaki.ilab.ps.matrix.factorization.workers

import hu.sztaki.ilab.ps.matrix.factorization.factors.{RangedRandomFactorInitializerDescriptor, SGDUpdater}
import hu.sztaki.ilab.ps.{ParameterServerClient, WorkerLogic}
import hu.sztaki.ilab.ps.matrix.factorization.utils.Rating
import hu.sztaki.ilab.ps.matrix.factorization.utils.Vector._
import hu.sztaki.ilab.ps.matrix.factorization.utils.Utils.{ItemId, UserId}

import scala.collection.mutable
import scala.util.Random

/**
  * Realize the worker logic for online matrix factorization with SGD
  *
  * @param numFactors Number of latent factors
  * @param rangeMin Lower bound of the random number generator
  * @param rangeMax Upper bound of the random number generator
  * @param learningRate Learning rate of SGD
  * @param negativeSampleRate Number of negative samples (Ratings with rate = 0) for each positive rating
  * @param userMemory The last #userMemory item seen by the user will not be generated as negative sample
  */
class PSOnlineMatrixFactorizationWorker(numFactors: Int,
                                        rangeMin: Double,
                                        rangeMax: Double,
                                        learningRate: Double,
                                        userMemory: Int,
                                        negativeSampleRate: Int)  extends WorkerLogic[Rating, Vector, (UserId, Vector)]{



  // initialization method and update method
  val factorInitDesc = RangedRandomFactorInitializerDescriptor(numFactors, rangeMin, rangeMax)
  val factorUpdate = new SGDUpdater(learningRate)

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
