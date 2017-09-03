package hu.sztaki.ilab.ps.matrix.factorization

import hu.sztaki.ilab.ps.FlinkParameterServer
import hu.sztaki.ilab.ps.matrix.factorization.utils.Vector._
import hu.sztaki.ilab.ps.matrix.factorization.pruning.{COORD, LEMPPruningStrategy}
import hu.sztaki.ilab.ps.matrix.factorization.utils.{CollectTopKFromEachWorker, IDGenerator, Rating, RichRating}
import hu.sztaki.ilab.ps.matrix.factorization.utils.Utils.{ItemId, TopKWorkerOutput, UserId}
import hu.sztaki.ilab.ps.matrix.factorization.utils.Vector.LengthAndVector
import hu.sztaki.ilab.ps.matrix.factorization.workers.{BaseMFWorkerLogic, PSTopKGeneratorWorker}
import hu.sztaki.ilab.ps.server.SimplePSLogic
import org.apache.flink.api.common.functions.{Partitioner, RichFlatMapFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

class PSTopKGenerator {

}

/**
  * A use-case for PS: TopK Generation with LEMP
  * The item vectors are stored at the workers and the user vectors at the parameter server.
  * Each rating in the Stream is broadcasted for every worker where everyone generates a local top k
  * for the given user. The local top k-s will be merged in the sink.
  *
  */

object PSTopKGenerator {

  /**
    *
    * @param model A flink DataStream of the model: (UserId, Param) / (ItemId, Param)
    * @param src A flink data stream containing [[utils.Rating]]s
    * @param numFactors Number of latent factors
    * @param rangeMin Lower bound of the random number generator
    * @param rangeMax Upper bound of the random number generator
    * @param userMemory The last #memory item seen by the user will not be recommended
    * @param K Number of items in the generated recommendation
    * @param workerK Number of items in the locally generated recommendations
    * @param bucketSize Parameter of the LEMP algorithm
    * @param pruningAlgorithm Pruning strategy based on the LEMP paper
    * @param pullLimit  Upper limit of unanswered pull requests in the system
    * @param workerParallelism Number of worker nodes
    * @param psParallelism Number of parameter server nodes
    * @param iterationWaitTime Time without new rating before shutting down the system (never stops if set to 0)
    * @return For each rating a (ItemId, TimeStamp, TopK List) tuple
    */
  def psTopKGenerator(src: DataStream[Rating],
                      model: DataStream[Either[(ItemId, LengthAndVector), (UserId, LengthAndVector)]],
                      numFactors: Int = 10,
                      rangeMin: Double = -0.01,
                      rangeMax: Double = 0.01,
                      userMemory: Int = 0,
                      K: Int = 100,
                      workerK: Int = 75,
                      bucketSize: Int = 100,
                      pruningAlgorithm: LEMPPruningStrategy = COORD(),
                      pullLimit: Int = 1600,
                      workerParallelism: Int = 4,
                      psParallelism: Int = 4,
                      iterationWaitTime: Int = 10000): DataStream[(ItemId, Long, List[(Double, ItemId)])] = {

    val invalidParam: LengthAndVector = (-1, new Array[VectorLength](0))

    val baseWorkerLogic = new PSTopKGeneratorWorker(
      workerK = workerK,
      bucketSize = bucketSize,
      workerParallelism = workerParallelism,
      pruning = pruningAlgorithm
    )

    val workerLogic: BaseMFWorkerLogic[RichRating, LengthAndVector, TopKWorkerOutput] =
      BaseMFWorkerLogic.addPullLimiter(baseWorkerLogic, pullLimit)

    val psLogic = new SimplePSLogic[LengthAndVector](
      _ => invalidParam, (_, x) => x
    )

    val partitionedInput = src.flatMap(new RichFlatMapFunction[Rating, RichRating] {
      override def flatMap(in: Rating, out: Collector[RichRating]) {
        val ratingId = IDGenerator.next
        for (i <- 0 until workerParallelism) {
          out.collect(in.enrich(i, ratingId))
        }
      }
    }).partitionCustom(new Partitioner[Int] {
      override def partition(key: Int, numPartitions: Int): Int = {
        key % numPartitions
      }
    }, x => x.targetWorker)

    val partitioner = new utils.Partitioner[LengthAndVector](psParallelism)

    FlinkParameterServer.transformWithDoubleModelLoad(model)(
      partitionedInput,
      workerLogic,
      psLogic,
      partitioner.workerToPSPartitioner,
      partitioner.psToWorkerPartitioner,
      workerParallelism,
      psParallelism,
      iterationWaitTime)
      .flatMap(new CollectTopKFromEachWorker(K, userMemory, workerParallelism)).setParallelism(1)
      .map( _ match {
        case (_, itemId, timestamp, topK) => (itemId, timestamp, topK)
      })

  }
}