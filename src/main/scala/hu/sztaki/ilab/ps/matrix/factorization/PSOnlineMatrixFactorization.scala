package hu.sztaki.ilab.ps.matrix.factorization

import hu.sztaki.ilab.ps.matrix.factorization.factors.RangedRandomFactorInitializerDescriptor
import hu.sztaki.ilab.ps.matrix.factorization.utils.Rating
import hu.sztaki.ilab.ps.matrix.factorization.utils.Utils.{ItemId, UserId}
import hu.sztaki.ilab.ps.matrix.factorization.utils.Vector._
import hu.sztaki.ilab.ps.matrix.factorization.workers.PSOnlineMatrixFactorizationWorker
import hu.sztaki.ilab.ps.server.SimplePSLogic
import hu.sztaki.ilab.ps.{FlinkParameterServer, WorkerLogic}
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._

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

    val workerLogicBase = new PSOnlineMatrixFactorizationWorker(numFactors, rangeMin, rangeMax, learningRate, negativeSampleRate, userMemory)

    val workerLogic: WorkerLogic[Rating, Vector, (UserId, Vector)] = WorkerLogic.addPullLimiter(workerLogicBase, pullLimit)

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
