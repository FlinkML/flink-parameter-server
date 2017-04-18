package hu.sztaki.ilab.ps.passive.aggressive

import breeze.linalg._
import hu.sztaki.ilab.ps.client.receiver.SimpleWorkerReceiver
import hu.sztaki.ilab.ps.client.sender.SimpleWorkerSender
import hu.sztaki.ilab.ps.entities.{PSToWorker, Pull, Push, WorkerToPS}
import hu.sztaki.ilab.ps.passive.aggressive.algorithm.PassiveAggressiveParameterInitializer
import hu.sztaki.ilab.ps.passive.aggressive.algorithm.binary.PassiveAggressiveClassification
import hu.sztaki.ilab.ps.server.SimplePSLogicWithClose
import hu.sztaki.ilab.ps.server.receiver.SimplePSReceiver
import hu.sztaki.ilab.ps.server.sender.SimplePSSender
import hu.sztaki.ilab.ps.{FlinkParameterServer, ParameterServerClient, WorkerLogic}
import org.apache.flink.streaming.api.scala._
import org.slf4j.LoggerFactory

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class PassiveAggressiveBinaryClassification


object PassiveAggressiveBinaryClassification {

  private val log = LoggerFactory.getLogger(classOf[PassiveAggressiveBinaryClassification])

  // A labeled data-point consisting of a sparse vector with a boolean label
  type LabeledVector = (SparseVector[Double], Boolean)
  type OptionLabeledVector = (SparseVector[Double], Option[Boolean])

  /**
    * Applies online binary classification for a [[org.apache.flink.streaming.api.scala.DataStream]] of sparse vectors.
    * For vectors with labels, it updates the model "passive-aggressively",
    * for vectors without label predicts its labels based on the model.
    *
    * Note that the order could be mixed, i.e. it's possible to predict based on some model parameters updated, while
    * others not.
    *
    * @param inputSource
    * [[org.apache.flink.streaming.api.scala.DataStream]] of labelled and unlabelled vector. The label is marked with an
    * [[Option]].
    * @param workerParallelism
    * Number of worker instances for Parameter Server.
    * @param psParallelism
    * Number of Parameter Server instances.
    * @param passiveAggressiveMethod
    * Method for Passive Aggressive training.
    * @param pullLimit
    * Limit of unanswered pulls at a worker instance.
    * @param iterationWaitTime
    * Time to wait for new messages at worker. If set to 0, the job will run infinitely.
    * PS is implemented with a Flink iteration, and Flink does not know when the iteration finishes,
    * so this is how the job can finish.
    * @return
    * Stream of Parameter Server model updates and predicted values.
    */
  def transform(inputSource: DataStream[OptionLabeledVector],
                workerParallelism: Int,
                psParallelism: Int,
                passiveAggressiveMethod: PassiveAggressiveClassification,
                pullLimit: Int,
                iterationWaitTime: Long): DataStream[Either[(SparseVector[Double], Boolean), (Int, Double)]] = {

    val serverLogic = new SimplePSLogicWithClose[Double](PassiveAggressiveParameterInitializer.init, _ + _)

    val workerLogic = WorkerLogic.addPullLimiter( // adding pull limiter to avoid iteration deadlock
      new WorkerLogic[OptionLabeledVector, Double, LabeledVector] {

        val paramWaitingQueue = new mutable.HashMap[Int, mutable.Queue[(OptionLabeledVector, ArrayBuffer[(Int, Double)])]]()

        override def onRecv(data: OptionLabeledVector,
                            ps: ParameterServerClient[Double, LabeledVector]): Unit = data match {
          // pulling parameters and buffering data while waiting for parameters
          case (vector: SparseVector[Double], label) =>
            val restedData = (vector, label)
            // buffer to store the already received parameters
            val waitingValues = new ArrayBuffer[(Int, Double)]()
            vector.activeKeysIterator.foreach(k => {
              paramWaitingQueue.getOrElseUpdate(k, mutable.Queue[(OptionLabeledVector, ArrayBuffer[(Int, Double)])]())
                .enqueue((restedData, waitingValues))
              ps.pull(k)
            })
        }

        override def onPullRecv(paramId: Int,
                                modelValue: Double,
                                ps: ParameterServerClient[Double, LabeledVector]) {
          // store the received parameters and train/predict when all corresponding parameters arrived for a vector
          val q = paramWaitingQueue(paramId)
          val (restedData, waitingValues) = q.dequeue()
          waitingValues += ((paramId, modelValue))
          if (waitingValues.size == restedData._1.activeSize) {
            // we have received all the parameters
            val (vector, optionLabel) = restedData

            val indexValuePair = ListMap(waitingValues.sorted: _*)
            val model = new SparseVector[Double](indexValuePair.keySet.toArray, indexValuePair.values.toArray,
              vector.length)

            optionLabel match {
              case Some(label) =>
                // we have a labelled vector, so we update the model
                passiveAggressiveMethod.delta(vector, model, if (label) 1 else -1)
                  .activeIterator
                  .foreach {
                    case (i, v) => ps.push(i, v)
                  }
              case None =>
                // we have an unlabelled vector, so we predict based on the model
                ps.output(vector, passiveAggressiveMethod.predict(vector, model) > 0)
            }
          }
          if (q.isEmpty) paramWaitingQueue.remove(paramId)
        }

      }, pullLimit)


    // hash partitioning the parameters
    val paramPartitioner: WorkerToPS[Double] => Int = {
      case WorkerToPS(partitionId, msg) => msg match {
        case Left(Pull(paramId)) => Math.abs(paramId) % psParallelism
        case Right(Push(paramId, delta)) => Math.abs(paramId) % psParallelism
      }
    }

    // hash partitioning the parameters
    val wInPartition: PSToWorker[Double] => Int = {
      case PSToWorker(workerPartitionIndex, msg) => workerPartitionIndex
    }


    val modelUpdates = FlinkParameterServer.parameterServerTransform(inputSource, workerLogic, serverLogic,
      paramPartitioner = paramPartitioner,
      wInPartition = wInPartition,
      workerParallelism,
      psParallelism,
      new SimpleWorkerReceiver[Double](),
      new SimpleWorkerSender[Double](),
      new SimplePSReceiver[Double](),
      new SimplePSSender[Double](),
      iterationWaitTime)

    modelUpdates
  }

}

