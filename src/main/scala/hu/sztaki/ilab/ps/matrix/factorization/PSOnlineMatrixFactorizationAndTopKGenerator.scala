package hu.sztaki.ilab.ps.matrix.factorization

import hu.sztaki.ilab.ps.{FlinkParameterServer, WorkerLogic}
import hu.sztaki.ilab.ps.matrix.factorization.factors.{RangedRandomFactorInitializerDescriptor, SGDUpdater}
import hu.sztaki.ilab.ps.matrix.factorization.pruning.{LEMPPruningStrategy, LI}
import hu.sztaki.ilab.ps.matrix.factorization.utils.InputTypes.{Rating, RichRating}
import hu.sztaki.ilab.ps.matrix.factorization.utils.{CollectTopKFromEachWorker, IDGenerator}
import hu.sztaki.ilab.ps.matrix.factorization.utils.Utils.{ItemId, UserId}
import hu.sztaki.ilab.ps.matrix.factorization.utils.Vector._
import hu.sztaki.ilab.ps.matrix.factorization.workers.PSOnlineMatrixFactorizationAndTopKGeneratorWorker
import hu.sztaki.ilab.ps.server.SimplePSLogic
import org.apache.flink.api.common.functions.{Partitioner, RichFlatMapFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


class PSOnlineMatrixFactorizationAndTopKGenerator{

}

/**
  * A use-case for PS: online matrix factorization with SGD and TopK generator with LEMP
  * The item vectors are stored at the workers and the user vectors at the parameter server.
  * Each rating in the Stream is broadcasted for every worker where everyone generates a local top k
  * for the given user. The local top k-s will be merged in the sink.
  * After the top k generation the worker node, where the item can be found, will calculate the SGD step
  * and pushing the updated parameter.
  *
  */
object PSOnlineMatrixFactorizationAndTopKGenerator {

  /**
    *
    * @param src                A flink data stream containing [[hu.sztaki.ilab.ps.matrix.factorization.utils.InputTypes.Rating]]s
    * @param numFactors         Number of latent factors
    * @param rangeMin           Lower bound of the random number generator
    * @param rangeMax           Upper bound of the random number generator
    * @param learningRate       Learning rate of SGD
    * @param negativeSampleRate Number of negative samples (Ratings with rate = 0) for each positive rating
    * @param userMemory         The last #memory item seen by the user will not be recommended
    * @param K                  Number of items in the generated recommendation
    * @param workerK Number of items in the locally generated recommendations
    * @param bucketSize Parameter of the LEMP algorithm
    * @param pruningAlgorithm Pruning strategy based on the LEMP paper
    * @param pullLimit  Upper limit of unanswered pull requests in the system
    * @param workerParallelism Number of workernodes
    * @param psParallelism Number of parameter server nodes
    * @param iterationWaitTime Time without new rating before shutting down the system (never stops if set to 0)
    * @return For each rating a (UserId, ItemId, TimeStamp, TopK List) tuple
    */
  def psOnlineLearnerAndGenerator(src: DataStream[Rating],
                                  numFactors: Int = 10,
                                  rangeMin: Double = -0.001,
                                  rangeMax: Double = 0.001,
                                  learningRate: Double,
                                  negativeSampleRate: Int,
                                  userMemory: Int = 65535,
                                  K: Int = 100,
                                  workerK: Int = 75,
                                  bucketSize: Int = 100,
                                  pruningAlgorithm: LEMPPruningStrategy = LI(5, 2.5),
                                  pullLimit: Int = 500,
                                  workerParallelism: Int = 4,
                                  psParallelism: Int = 4,
                                  iterationWaitTime: Long = 10000): DataStream[(UserId, ItemId, Long, List[(Double, ItemId)])] = {


    val factorInitDesc = RangedRandomFactorInitializerDescriptor(numFactors, rangeMin, rangeMax)

    val baseWorkerLogic = new PSOnlineMatrixFactorizationAndTopKGeneratorWorker(
      negativeSampleRate = negativeSampleRate,
      userMemory = userMemory,
      workerK = workerK,
      bucketSize = bucketSize,
      pruningAlgorithm = pruningAlgorithm,
      workerParallelism = workerParallelism,
      factorInitDesc = factorInitDesc,
      factorUpdate = new SGDUpdater(learningRate))

    val workerLogic = WorkerLogic.addPullLimiter(baseWorkerLogic, pullLimit)

    val serverLogic = new SimplePSLogic[UserId, LengthAndVector](
      x => attachLength(factorInitDesc.open().nextFactor(x)),
      { (vec, deltaVec) => attachLength(vectorSum(vec._2, deltaVec._2))}
    )

    val partitionedInput = src.flatMap(new RichFlatMapFunction[Rating, RichRating] {
      override def flatMap(in: Rating, out: Collector[RichRating]) {
        val ratingId = IDGenerator.next
        for (i <- 0 until workerParallelism) {
          out.collect(in.enrich(i, ratingId))
        }
      }
    }).partitionCustom(new Partitioner[Int] {
      override def partition(key: Int, numPartitions: Int): ItemId = { key % numPartitions }
    }, x => x.targetWorker)

    FlinkParameterServer.transform(
      partitionedInput, workerLogic, serverLogic, workerParallelism, psParallelism, iterationWaitTime)
      .flatMap( new CollectTopKFromEachWorker(K, userMemory, workerParallelism)).setParallelism(1)
  }

}
