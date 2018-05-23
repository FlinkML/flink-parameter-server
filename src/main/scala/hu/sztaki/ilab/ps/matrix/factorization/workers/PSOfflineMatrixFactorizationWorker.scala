package hu.sztaki.ilab.ps.matrix.factorization.workers

import hu.sztaki.ilab.ps.matrix.factorization.PSOfflineMatrixFactorization
import hu.sztaki.ilab.ps.matrix.factorization.factors.{FactorInitializer, RangedRandomFactorInitializerDescriptor, SGDUpdater}
import hu.sztaki.ilab.ps.matrix.factorization.utils.{EOF, InputTypes}
import hu.sztaki.ilab.ps.matrix.factorization.utils.InputTypes.Rating
import hu.sztaki.ilab.ps.matrix.factorization.utils.Utils.{ItemId, UserId}
import hu.sztaki.ilab.ps.matrix.factorization.utils.Vector.{Vector, vectorSum}
import hu.sztaki.ilab.ps.{ParameterServerClient, WorkerLogic}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random


/**
  * Realize the worker logic for offline matrix factorization with SGD
  *
  * @param numFactors Number of latent factors
  * @param learningRate  Learning rate of SGD
  * @param rangeMin Lower bound of the random number generator
  * @param rangeMax Upper bound of the random number generator
  * @param negativeSampleRate  Number of negative samples (Ratings with rate = 0) for each positive rating
  * @param userMemory The last #memory item seen by the user will not be generated as negative sample
  * @param iterations How many times will it iterate through the whole data
  */
class PSOfflineMatrixFactorizationWorker(numFactors: Int,
                                         rangeMin: Double,
                                         rangeMax: Double,
                                         learningRate: Double,
                                         negativeSampleRate: Int,
                                         userMemory: Int,
                                         iterations: Int) extends WorkerLogic[Either[EOF, Rating], Vector, (UserId, Vector)]{

  private val log = LoggerFactory.getLogger(classOf[PSOfflineMatrixFactorization])


  // initialization method and update method
  val factorInitDesc = RangedRandomFactorInitializerDescriptor(numFactors, rangeMin, rangeMax)
  val factorUpdate = new SGDUpdater(learningRate) // fixme add lambda

  val rbs = new ArrayBuffer[ArrayBuffer[Rating]]()
  val userVectors = new mutable.HashMap[UserId, Vector]()

  val ratingBuffer = new mutable.HashMap[ItemId, mutable.Queue[(UserId, Double)]]()
  val seenItemsSet = new mutable.HashMap[UserId, mutable.HashSet[ItemId]]
  val seenItemsQueue = new mutable.HashMap[UserId, mutable.Queue[ItemId]]
  val allItemIdsSet = new mutable.HashSet[ItemId]
  val allItemIdsArray = new mutable.ArrayBuffer[ItemId]

  @transient
  lazy val factorInit: FactorInitializer = factorInitDesc.open()

  // todo gracefully stop thread at end of computation (override close method)
  var workerThread: Thread = null

  // We need to check if all threads finished already
  var EOFsReceived = 0

  override def onRecv(value: Either[EOF, Rating],
                      ps: ParameterServerClient[Vector, (UserId, Vector)]): Unit = {

    value match {
      case Right(rating) =>
        if (workerThread != null) {
          throw new IllegalStateException("Should not have started worker thread while waiting for further " +
            "elements.")
        }
        if (!(allItemIdsSet contains rating.item)) {
          allItemIdsSet += rating.item
          allItemIdsArray += rating.item
        }

        val rs = new ArrayBuffer[Rating]()

        val seenSet = seenItemsSet.getOrElseUpdate(rating.user, new mutable.HashSet)
        val seenQueue = seenItemsQueue.getOrElseUpdate(rating.user, new mutable.Queue)
        if (seenQueue.length >= userMemory) {
          seenSet -= seenQueue.dequeue()
        }
        seenSet += rating.item
        seenQueue += rating.item

        for(_  <- 1 to Math.min(allItemIdsSet.size - seenSet.size, negativeSampleRate)) {
          var randomItemId = allItemIdsArray(Random.nextInt(allItemIdsArray.length))

          while (seenSet contains randomItemId) {
            randomItemId = allItemIdsArray(Random.nextInt(allItemIdsArray.length))
          }

          rs += InputTypes.ratingFromTuple(rating.user, randomItemId, 0.0)
        }

        rs += rating
        rbs += rs
      case Left(EOF()) =>
        // This marks the end of input. We can do the work.
        log.info(s"Number of received blocks of ratings: ${rbs.length}")

        // We start a new Thread to avoid blocking the answers to pulls.
        // Otherwise the work would only start when all the pulls are sent (for all iterations).
        workerThread = new Thread(new Runnable {
          override def run(): Unit = {

            log.debug("worker thread started")

            for (_ <- 1 to iterations) {
              Random.shuffle(rbs)

              for (rs <- rbs) {
                for (rating <- rs) {

                  // to avoid concurrent modification of the stored ratings
                  ratingBuffer synchronized {
                    ratingBuffer.getOrElseUpdate(rating.item, mutable.Queue[(UserId, Double)]())
                      .enqueue((rating.user, rating.rating))
                  }

                  // we assume that the PS client is thread safe, so we can use it from different threads
                  ps.pull(rating.item)
                }
              }
            }
            log.debug("pulls finished")
          }
        })
        workerThread.start()
    }
  }

  override def onPullRecv(item: ItemId,
                          itemVec: Vector,
                          ps: ParameterServerClient[Vector, (UserId, Vector)]): Unit = {
    // to avoid concurrent modification of the stored ratings
    val (user, rating) = ratingBuffer synchronized {
      ratingBuffer(item).dequeue()
    }

    val userVec = userVectors.getOrElseUpdate(user, factorInit.nextFactor(user))
    val (deltaUserVec, deltaItemVec) = factorUpdate.delta(rating, userVec, itemVec)
    userVectors(user) = vectorSum(userVec, deltaUserVec)

    // we assume that the PS client is thread safe, so we can use it from different threads
    ps.output((user, userVectors(user)))
    ps.push(item, deltaItemVec)
  }

  override def close(): Unit = {
  }
}
