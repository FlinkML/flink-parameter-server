package hu.sztaki.ilab.ps.matrix.factorization

import hu.sztaki.ilab.ps.matrix.factorization.factors.RangedRandomFactorInitializerDescriptor
import hu.sztaki.ilab.ps.matrix.factorization.utils.{EOF, Rating}
import hu.sztaki.ilab.ps.matrix.factorization.utils.Utils.{ItemId, UserId}
import hu.sztaki.ilab.ps.matrix.factorization.utils.Vector._
import hu.sztaki.ilab.ps.matrix.factorization.workers.PSOfflineMatrixFactorizationWorker
import hu.sztaki.ilab.ps.{FlinkParameterServer, WorkerLogic}
import org.apache.flink.api.common.functions.{Partitioner, RichFlatMapFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory


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


  /**
    * @param src A flink data stream containing [[utils.Rating]]s
    * @param numFactors Number of latent factors
    * @param learningRate  Learning rate of SGD
    * @param rangeMin Lower bound of the random number generator
    * @param rangeMax Upper bound of the random number generator
    * @param negativeSampleRate  Number of negative samples (Ratings with rate = 0) for each positive rating
    * @param userMemory The last #memory item seen by the user will not be generated as negative sample
    * @param iterations How many times will it iterate through the whole data
    * @param pullLimit Upper limit of unanswered pull requests in the system
    * @param workerParallelism Number of workernodes
    * @param psParallelism Number of parameter server nodes
    * @param iterationWaitTime Time without new rating before shutting down the system (never stops if set to 0)
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

    val workerLogicBase = new PSOfflineMatrixFactorizationWorker(
      numFactors,
      rangeMin,
      rangeMax,
      learningRate,
      negativeSampleRate,
      userMemory,
      iterations)

    val workerLogic: WorkerLogic[Either[EOF, Rating], Vector, (UserId, Vector)] =
      WorkerLogic.addPullLimiter(workerLogicBase, pullLimit)

    val paramInit = (id: Int) => factorInitDesc.open().nextFactor(id)
    val paramUpdate: (Vector, Vector) => Vector = {
      case (vec, deltaVec) => vectorSum(vec, deltaVec)
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