package hu.sztaki.ilab.ps.matrix.factorization

import hu.sztaki.ilab.ps.matrix.factorization.factors.{FactorInitializer, RangedRandomFactorInitializerDescriptor, SGDUpdater}
import hu.sztaki.ilab.ps.matrix.factorization.utils.Rating
import hu.sztaki.ilab.ps.matrix.factorization.utils.Utils.{ItemId, UserId}
import hu.sztaki.ilab.ps.matrix.factorization.utils.Vector._
import hu.sztaki.ilab.ps.{FlinkParameterServer, ParameterServerClient, WorkerLogic}
import org.apache.flink.api.common.functions.{Partitioner, RichFlatMapFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random


class PSOfflineMatrixFactorization {
}

/**
  * A use-case for PS: matrix factorization with SGD.
  * Reads data into workers, then iterates on it, storing the user vectors at the worker and item vectors at the PS.
  * At every iteration, the worker pulls the relevant item vectors, applies the SGD updates and pushes the updates.
  *
  */
object PSOfflineMatrixFactorization {

  private val log = LoggerFactory.getLogger(classOf[PSOfflineMatrixFactorization])

  case class EOF() extends Serializable

  /**
    * @param src A flink data stream containing [[utils.Rating]]s
    * @param numFactors Number of latent factors
    * @param learningRate  Learning rate of SGD
    * @param rangeMin Lower bound of the random number generator
    * @param rangeMax Upper bound of the random number generator
    * @param negativeSampleRate  Number of negative samples (Ratings with rate = 0) for each positive rating
    * @param iterations How many times will it iterate through the whole data
    * @param pullLimit Upper limit of unanswered pull requests in the system
    * @param workerParallelism Number of workernodes
    * @param psParallelism Number of parameter server nodes
    * @param iterationWaitTime Time without new rating before shutting down the system (never stops if set to 0)
    * @param userMemory The last #memory item seen by the user will not be generated as negative sample
    * @return For each rating the updated (userId, userVectors) / (itemId, Vectors) tuples
    */

  def psOfflineMF(src: DataStream[Rating],
                  numFactors: Int = 10,
                  rangeMin: Double = -0.01,
                  rangeMax: Double = 0.01,
                  learningRate: Double,
                  negativeSampleRate: Int = 0,
                  userMemory: Int = 128,
                  iterations: Int,
                  pullLimit: Int = 1600,
                  workerParallelism: Int,
                  psParallelism: Int,
                  iterationWaitTime: Long = 10000): DataStream[Either[(UserId, Vector), (ItemId, Vector)]] = {

    import hu.sztaki.ilab.ps.utils.FlinkEOF._

    val ratings: DataStream[Either[EOF, Rating]] = flatMapWithEOF(src,
      new RichFlatMapFunction[Rating, Either[EOF, Rating]] with EOFHandler[Either[EOF, Rating]] {
        override def flatMap(value: (Rating), out: Collector[Either[EOF, (Rating)]]): Unit = {
          out.collect(Right(value))
        }

        override def onEOF(collector: Collector[Either[EOF, (Rating)]]): Unit = {
          collector.collect(Left(EOF()))
        }
      },
      workerParallelism,
      new Partitioner[UserId] {
        override def partition(key: UserId, numPartitions: Int): Int = key % numPartitions
      },
      (x: Rating) => x.user
    )

    // initialization method and update method
    val factorInitDesc = RangedRandomFactorInitializerDescriptor(numFactors, rangeMin, rangeMax)

    // fixme add lambda
    val factorUpdate = new SGDUpdater(learningRate)

    val workerLogicBase = new WorkerLogic[Either[EOF, Rating], Vector, (UserId, Vector)] {

      val rbs = new ArrayBuffer[ArrayBuffer[Rating]]()
      val userVectors = new mutable.HashMap[UserId, Vector]()

      val itemRatings = new mutable.HashMap[ItemId, mutable.Queue[(UserId, Double)]]()
      val itemIdsSeenByUser = new mutable.HashMap[UserId, mutable.HashSet[ItemId]]
      val itemIdsSeenByUserQueue = new mutable.HashMap[UserId, mutable.Queue[ItemId]]
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

            val seenSet = itemIdsSeenByUser.getOrElseUpdate(rating.user, new mutable.HashSet)
            val seenQueue = itemIdsSeenByUserQueue.getOrElseUpdate(rating.user, new mutable.Queue)
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

              rs += Rating.fromTuple(rating.user, randomItemId, 0.0)
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
                      itemRatings synchronized {
                        itemRatings.getOrElseUpdate(rating.item, mutable.Queue[(UserId, Double)]())
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
        val (user, rating) = itemRatings synchronized {
          itemRatings(item).dequeue()
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

    val workerLogic: WorkerLogic[Either[EOF, Rating], Vector, (UserId, Vector)] =
      WorkerLogic.addBlockingPullLimiter(workerLogicBase, pullLimit)

    val paramInit = (id: Int) => factorInitDesc.open().nextFactor(id)
    val paramUpdate: (Vector, Vector) => Vector = {
      case (vec, deltaVec) => vec.zip(deltaVec).map(x => x._1 + x._2)
    }

    val modelUpdates = FlinkParameterServer.transform(
      ratings,
      workerLogic,
      paramInit, paramUpdate,
      workerParallelism,
      psParallelism,
      iterationWaitTime)

    modelUpdates
  }

}