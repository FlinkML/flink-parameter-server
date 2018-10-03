package hu.sztaki.ilab.ps.matrix.factorization.workers

import hu.sztaki.ilab.ps.matrix.factorization.factors.{FactorInitializerDescriptor, FactorUpdater}
import hu.sztaki.ilab.ps.matrix.factorization.pruning.LEMPPruningFunctions._
import hu.sztaki.ilab.ps.matrix.factorization.pruning._
import hu.sztaki.ilab.ps.matrix.factorization.utils.InputTypes.RichRating
import hu.sztaki.ilab.ps.matrix.factorization.utils.Utils._
import hu.sztaki.ilab.ps.matrix.factorization.utils.Vector._
import hu.sztaki.ilab.ps.{ParameterServerClient, WorkerLogic}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random


/**
  * Worker logic for online matrix factorization and Top K generation
  *
  * @param negativeSampleRate Number of negative samples (Ratings with rate = 0) for each positive rating
  * @param userMemory         The last #memory item seen by the user will not be recommended
  * @param workerK Number of items in the locally generated recommendations
  * @param bucketSize Parameter of the LEMP algorithm
  * @param pruningAlgorithm Pruning strategy based on the LEMP paper
  * @param workerParallelism Number of worker nodes
  * @param factorInitDesc Specifies how the new latent vectors should be initialized
  * @param factorUpdate Specifies the SGD update step
  */
class PSOnlineMatrixFactorizationAndTopKGeneratorWorker(negativeSampleRate: Int,
                                                        userMemory: Int,
                                                        workerK: Int,
                                                        bucketSize: Int,
                                                        pruningAlgorithm: LEMPPruningStrategy,
                                                        workerParallelism: Int,
                                                        factorInitDesc: FactorInitializerDescriptor,
                                                        factorUpdate: FactorUpdater)
  extends WorkerLogic[RichRating, UserId, LengthAndVector, TopKWorkerOutput] {

  val model = new mutable.HashMap[ItemId, LengthAndVector]()

  val itemIdsDescendingByLength = new mutable.TreeSet[(Double, ItemId)]()(implicitly[Ordering[(Double, ItemId)]].reverse)
  val itemIdsBuffer = new ArrayBuffer[ItemId] // needed for negative sample generation, where random access to a random element is required
  val ratingBuffer = new mutable.HashMap[UserId, mutable.Queue[RichRating]]()
  val seenList = new mutable.HashMap[UserId, mutable.Queue[ItemId]]
  val seenSet = new mutable.HashMap[UserId, mutable.HashSet[ItemId]]

  var workerId: Int = -1

  override def onRecv(data: RichRating,
                      ps: ParameterServerClient[UserId, LengthAndVector, TopKWorkerOutput]): Unit = {
    if (workerId == -1) {
      workerId = data.targetWorker
    }
    ratingBuffer synchronized {
      ratingBuffer.getOrElseUpdate(data.base.user, mutable.Queue[RichRating]()).enqueue(data)
    }
    ps.pull(data.base.user)
  }

  override def onPullRecv(paramId: UserId, userAndLen: LengthAndVector,
                          ps: ParameterServerClient[UserId, LengthAndVector, TopKWorkerOutput]): Unit = {
    val rate = ratingBuffer synchronized {
      ratingBuffer(paramId).dequeue()
    }

    val (userVectorLength, userVector) = userAndLen

    val buckets = itemIdsDescendingByLength.toList.grouped(bucketSize)

    val topK = new mutable.PriorityQueue[(Double, ItemId)]()(implicitly[Ordering[(Double, ItemId)]].reverse)

    // focus coordinate for coord pruning test
    val focus = Array.range(0, userVector.length)
      .maxBy { x => userVector(x) * userVector(x) }

    // focus coordinate set for incremental pruning test
    val focusSet = Array.range(0, userVector.length - 1)
      .sortBy{ x => -userVector(x) * userVector(x) }
      .take(pruningAlgorithm match {
        case INCR(x)=> x
        case LI(x, _)=> x
        case _=> 0
      })

    var currentBucket: List[(Double, ItemId)] = null

    while (buckets.hasNext && {
      currentBucket = buckets.next

      (topK.length < workerK) || (currentBucket.head._1 * userVectorLength > topK.head._1)
    }) {

      val theta = if (topK.length < workerK) 0.0 else topK.head._1
      val theta_b_q = theta / (currentBucket.head._1 * userVectorLength)
      val vectors = currentBucket.map(x => (x._2, model(x._2)))

      val candidates = vectors.filter(pruningAlgorithm match {
        case LENGTH() => lengthPruning(theta / userVectorLength)
        case COORD() => coordPruning(focus, userAndLen, theta_b_q)
        case INCR(_) => incrPruning(focusSet, userAndLen, theta)
        case LC(threshold) =>
          if (currentBucket.head._1 > currentBucket.last._1 * threshold)
            lengthPruning(theta / userVectorLength)
          else
            coordPruning(focus, userAndLen, theta_b_q)
        case LI(_, threshold) =>
          if (currentBucket.head._1 > currentBucket.last._1 * threshold)
            lengthPruning(theta / userVectorLength)
          else
            incrPruning(focusSet, userAndLen, theta)
      })

      for (item <- candidates) {
        val userItemDotProduct = dotProduct(userVector, item._2._2)

        if (topK.size < workerK) {
          topK += ((userItemDotProduct, item._1))
        }
        else {
          if (topK.head._1 < userItemDotProduct) {
            topK.dequeue
            topK += ((userItemDotProduct, item._1))
          }
        }
      }
    }
    ps.output((rate, topK))

    if (rate.base.item.hashCode() % workerParallelism == workerId) {
      val numFactors = userVector.length

      val set = seenSet.getOrElseUpdate(rate.base.user, new mutable.HashSet)
      if (!(set contains rate.base.item)) {
        set += rate.base.item
        val list = seenList.getOrElseUpdate(rate.base.user, new mutable.Queue)
        list += rate.base.item
        if (list.length > userMemory) {
          set -= list.dequeue()
        }
      }

      // negative sample
      var uDelta = new Vector(numFactors)
      for (_ <- 0 until Math.min(model.size - set.size, negativeSampleRate)) {
        var negItemId = itemIdsBuffer(Random.nextInt(itemIdsBuffer.length))
        var counter = 32
        while ((counter > 0) && (set contains negItemId)) {
          negItemId = itemIdsBuffer(Random.nextInt(itemIdsBuffer.length))
          counter -= 1
        }
        if (counter > 0) {
          val (_, negItemValue) = model(negItemId)
          val (uuDelta, iDelta) = factorUpdate.delta(0.0, userVector, negItemValue)
          uDelta = vectorSum(uDelta, uuDelta)
          updateModelNoInit(negItemId, attachLength(vectorSum(negItemValue, iDelta)))
        }
      }

      val (_, itemVector) = model.getOrElse(rate.base.item, initialize(rate.base.item))
      val (userDelta, itemDelta) = factorUpdate.delta(rate.base.rating, userVector, itemVector)

      updateModelNoInit(rate.base.item, attachLength(vectorSum(itemVector, itemDelta)))

      ps.push(paramId, (Double.NaN, vectorSum(uDelta, userDelta)))
    }

  }


  def updateModelNoInit(id: ItemId, param: LengthAndVector): Unit = {
    itemIdsDescendingByLength -= ((model(id)._1, id))
    val (newLength, _) = param
    model += ((id, param))
    itemIdsDescendingByLength += ((newLength, id))
  }

  // used for initialisation
  def updateModel(id: ItemId, param: LengthAndVector): Unit = {
    if (model.contains(id)) {
      itemIdsDescendingByLength -= ((model(id)._1, id))
    }
    else {
      itemIdsBuffer += id
    }
    val (newLength, _) = param
    model += ((id, param))
    itemIdsDescendingByLength += ((newLength, id))
  }

  def initialize(id: ItemId): LengthAndVector = {
    val lenAndVector = attachLength(factorInitDesc.open().nextFactor(id))
    updateModel(id, lenAndVector)
    lenAndVector
  }

}
