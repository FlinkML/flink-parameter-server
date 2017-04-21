package hu.sztaki.ilab.ps.matrix.factorization

import hu.sztaki.ilab.ps.matrix.factorization.Utils._
import hu.sztaki.ilab.ps.matrix.factorization.factors.{FactorInitializer, RandomFactorInitializerDescriptor, SGDUpdater}
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

  // A user rates an item with a Double rating
  type Rating = (UserId, ItemId, Double)
  type Vector = Array[Double]

  def psOfflineMF(src: DataStream[Rating],
                  numFactors: Int,
                  learningRate: Double,
                  iterations: Int,
                  pullLimit: Int,
                  workerParallelism: Int,
                  psParallelism: Int,
                  iterationWaitTime: Long): DataStream[Either[(UserId, Vector), (ItemId, Vector)]] = {

    val readParallelism = src.parallelism

    import hu.sztaki.ilab.ps.utils.FlinkEOF._

    val ratings: DataStream[Either[EOF, Rating]] = flatMapWithEOF(src,
      new RichFlatMapFunction[Rating, Either[EOF, Rating]] with EOFHandler[Either[EOF, Rating]] {
        override def flatMap(value: (UserId, ItemId, Double), out: Collector[Either[EOF, (UserId, ItemId, Double)]]): Unit = {
          out.collect(Right(value))
        }

        override def onEOF(collector: Collector[Either[EOF, (UserId, ItemId, Double)]]): Unit = {
          collector.collect(Left(EOF()))
        }
      },
      workerParallelism,
      new Partitioner[UserId] {
        override def partition(key: UserId, numPartitions: Int): Int = key % numPartitions
      },
      (x: Rating) => x._1
    )

    // initialization method and update method
    val factorInitDesc = RandomFactorInitializerDescriptor(numFactors)

    // fixme add lambda
    val factorUpdate = new SGDUpdater(learningRate)

    val workerLogicBase = new WorkerLogic[Either[EOF, Rating], Vector, (UserId, Vector)] {

      val rs = new ArrayBuffer[Rating]()
      val userVectors = new mutable.HashMap[UserId, Vector]()

      val itemRatings = new mutable.HashMap[ItemId, mutable.Queue[(UserId, Double)]]()

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

            rs.append(rating)
          case Left(EOF()) =>
            // This marks the end of input. We can do the work.
            log.info(s"Number of received ratings: ${rs.length}")

            // We start a new Thread to avoid blocking the answers to pulls.
            // Otherwise the work would only start when all the pulls are sent (for all iterations).
            workerThread = new Thread(new Runnable {
              override def run(): Unit = {
                log.debug("worker thread started")
                for (iter <- 1 to iterations) {
                  Random.shuffle(rs)
                  for ((u, i, r) <- rs) {
                    // to avoid concurrent modification of the stored ratings
                    itemRatings synchronized {
                      itemRatings.getOrElseUpdate(i, mutable.Queue[(UserId, Double)]()).enqueue((u, r))
                    }

                    // we assume that the PS client is thread safe, so we can use it from different threads
                    ps.pull(i)
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
        userVectors(user) = userVec.zip(deltaUserVec).map(x => x._1 + x._2)

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

    val modelUpdates = FlinkParameterServer.parameterServerTransform(
      ratings,
      workerLogic,
      paramInit, paramUpdate,
      workerParallelism,
      psParallelism,
      iterationWaitTime)

    modelUpdates
  }

}
