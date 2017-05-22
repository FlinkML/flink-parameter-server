package hu.sztaki.ilab.ps.passive.aggressive

import breeze.linalg._
import hu.sztaki.ilab.ps.client.receiver.SimpleWorkerReceiver
import hu.sztaki.ilab.ps.client.sender.SimpleWorkerSender
import hu.sztaki.ilab.ps.entities.{PSToWorker, Pull, Push, WorkerToPS}
import hu.sztaki.ilab.ps.passive.aggressive.algorithm.{PassiveAggressiveAlgorithm, PassiveAggressiveParameterInitializer}
import hu.sztaki.ilab.ps.passive.aggressive.algorithm.PassiveAggressiveParameterInitializer._
import hu.sztaki.ilab.ps.server.{RangePSLogicWithClose, SimplePSLogicWithClose}
import hu.sztaki.ilab.ps.server.receiver.SimplePSReceiver
import hu.sztaki.ilab.ps.server.sender.SimplePSSender
import hu.sztaki.ilab.ps.{FlinkParameterServer, ParameterServerClient, WorkerLogic}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class PassiveAggressiveParameterServer


object PassiveAggressiveParameterServer {

  private val log = LoggerFactory.getLogger(classOf[PassiveAggressiveParameterServer])

  // A labeled data-point consisting of a sparse vector with a boolean label
  //  type LabeledVector = (SparseVector[Double], Boolean)
  //  type OptionLabeledVector = (SparseVector[Double], Option[Boolean])

  type FeatureId = Int

  /**
    * Applies online multiclass classification for a [[org.apache.flink.streaming.api.scala.DataStream]] of sparse
    * vectors. For vectors with labels, it updates the model "passive-aggressively",
    * for vectors without label predicts its labels based on the model.
    *
    * Labels should be indexed from 0.
    *
    * Note that the order could be mixed, i.e. it's possible to predict based on some model parameters updated, while
    * others not.
    *
    * @param inputSource
    * [[org.apache.flink.streaming.api.scala.DataStream]] of labelled and unlabelled vector.
    * The label is marked with an [[Option]].
    * @param workerParallelism
    * Number of worker instances for Parameter Server.
    * @param psParallelism
    * Number of Parameter Server instances.
    * @param passiveAggressiveMethod
    * Method for Passive Aggressive training.
    * @param pullLimit
    * Limit of unanswered pulls at a worker instance.
    * @param labelCount
    * Number of possible labels.
    * @param iterationWaitTime
    * Time to wait for new messages at worker. If set to 0, the job will run infinitely.
    * PS is implemented with a Flink iteration, and Flink does not know when the iteration finishes,
    * so this is how the job can finish.
    * @return
    * Stream of Parameter Server model updates and predicted values.
    */
  def transformMulticlass(inputSource: DataStream[(SparseVector[Double], Option[Int])],
                          workerParallelism: Int,
                          psParallelism: Int,
                          passiveAggressiveMethod: PassiveAggressiveAlgorithm[Vector[Double], Int, CSCMatrix[Double]],
                          pullLimit: Int,
                          labelCount: Int,
                          featureCount: Int,
                          rangePartitioning: Boolean,
                          iterationWaitTime: Long)
  : DataStream[Either[(SparseVector[Double], Int), (FeatureId, Vector[Double])]] = {
    val multiModelBuilder = new ModelBuilder[Vector[Double], CSCMatrix[Double]] {
      override def buildModel(params: Iterable[(FeatureId, Vector[Double])],
                              featureCount: Int): CSCMatrix[Double] = {
        val builder = new CSCMatrix.Builder[Double](featureCount, labelCount)
        params.foreach { case (i, vector) => vector.foreachPair((j, v) => builder.add(i, j, v)) }

        builder.result
      }
    }

    transformGeneric[Vector[Double], Int, CSCMatrix[Double]](
      initMulti(labelCount), _ + _, multiModelBuilder
    )(
      inputSource, workerParallelism, psParallelism, passiveAggressiveMethod,
      pullLimit, labelCount, featureCount, rangePartitioning, iterationWaitTime
    )
  }

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
  def transformBinary(inputSource: DataStream[(SparseVector[Double], Option[Boolean])],
                      workerParallelism: Int,
                      psParallelism: Int,
                      passiveAggressiveMethod: PassiveAggressiveAlgorithm[Double, Boolean, Vector[Double]],
                      pullLimit: Int,
                      featureCount: Int,
                      rangePartitioning: Boolean,
                      iterationWaitTime: Long)
  : DataStream[Either[(SparseVector[Double], Boolean), (FeatureId, Double)]] = {
    val labelCount = 1

    val binaryModelBuilder = new ModelBuilder[Double, Vector[Double]] {
      override def buildModel(params: Iterable[(FeatureId, Double)],
                              featureCount: Int): Vector[Double] = {
        // TODO check whether it works without creating an array and sorting. It should.
        val indexValuePair = params.toArray.sortBy(_._1)
        val model = SparseVector[Double](featureCount)(indexValuePair: _*)

        model
      }
    }

    transformGeneric[Double, Boolean, Vector[Double]](
      initBinary, _ + _, binaryModelBuilder
    )(
      inputSource, workerParallelism, psParallelism, passiveAggressiveMethod,
      pullLimit, labelCount, featureCount, rangePartitioning, iterationWaitTime
    )
  }

  private def transformGeneric
  [Param, Label, Model](init: Int => Param,
                        add: (Param, Param) => Param,
                        modelBuilder: ModelBuilder[Param, Model])
                       (inputSource: DataStream[(SparseVector[Double], Option[Label])],
                        workerParallelism: Int,
                        psParallelism: Int,
                        passiveAggressiveMethod: PassiveAggressiveAlgorithm[Param, Label, Model],
                        pullLimit: Int,
                        labelCount: Int,
                        featureCount: Int,
                        // TODO avoid using boolean
                        rangePartitioning: Boolean,
                        iterationWaitTime: Long)
                       (implicit
                        tiParam: TypeInformation[Param],
                        tiLabel: TypeInformation[Label])
  : DataStream[Either[(SparseVector[Double], Label), (FeatureId, Param)]]

  = {

    type LabeledVector = (SparseVector[Double], Label)
    type OptionLabeledVector = (SparseVector[Double], Option[Label])

    val serverLogic =
      if (rangePartitioning) {
        new RangePSLogicWithClose[Param](featureCount, init, add)
      } else {
        new SimplePSLogicWithClose[Param](init, add)
      }

    val paramPartitioner: WorkerToPS[Param] => Int =
      if (rangePartitioning) {
        rangePartitionerPS(featureCount)(psParallelism)
      } else {
        val partitonerFunction = (paramId: Int) => Math.abs(paramId) % psParallelism
        val p: WorkerToPS[Param] => Int = {
          case WorkerToPS(partitionId, msg) => msg match {
            case Left(Pull(paramId)) => partitonerFunction(paramId)
            case Right(Push(paramId, delta)) => partitonerFunction(paramId)
          }
        }
        p
      }

    val workerLogic = WorkerLogic.addPullLimiter( // adding pull limiter to avoid iteration deadlock
      new WorkerLogic[OptionLabeledVector, Param, LabeledVector] {

        val paramWaitingQueue = new mutable.HashMap[Int, mutable.Queue[(OptionLabeledVector, ArrayBuffer[(Int, Param)])]]()

        override def onRecv(data: OptionLabeledVector,
                            ps: ParameterServerClient[Param, LabeledVector]): Unit = data match {
          // pulling parameters and buffering data while waiting for parameters
          case (vector: SparseVector[Double], label) =>
            val restedData = (vector, label)
            // buffer to store the already received parameters
            val waitingValues = new ArrayBuffer[(Int, Param)]()
            vector.activeKeysIterator.foreach(k => {
              paramWaitingQueue.getOrElseUpdate(k, mutable.Queue[(OptionLabeledVector, ArrayBuffer[(Int, Param)])]())
                .enqueue((restedData, waitingValues))
              ps.pull(k)
            })
        }

        override def onPullRecv(paramId: Int,
                                modelValue: Param,
                                ps: ParameterServerClient[Param, LabeledVector]) {
          // store the received parameters and train/predict when all corresponding parameters arrived for a vector
          val q = paramWaitingQueue(paramId)
          val (restedData, waitingValues) = q.dequeue()
          waitingValues += paramId -> modelValue
          if (waitingValues.size == restedData._1.activeSize) {
            // we have received all the parameters
            val (vector, optionLabel) = restedData

            val model: Model = modelBuilder.buildModel(waitingValues, vector.length)

            optionLabel match {
              case Some(label) =>
                // we have a labelled vector, so we update the model
                passiveAggressiveMethod.delta(vector, model, label)
                  .foreach {
                    case (i, v) => ps.push(i, v)
                  }
              case None =>
                // we have an unlabelled vector, so we predict based on the model
                ps.output(vector, passiveAggressiveMethod.predict(vector, model))
            }
          }
          if (q.isEmpty) paramWaitingQueue.remove(paramId)
        }

      }, pullLimit)


    val wInPartition: PSToWorker[Param] => Int = {
      case PSToWorker(workerPartitionIndex, msg) => workerPartitionIndex
    }

    val modelUpdates = FlinkParameterServer.parameterServerTransform[
      OptionLabeledVector, Param, (FeatureId, Param),
      (SparseVector[Double], Label), PSToWorker[Param], WorkerToPS[Param]](
      inputSource, workerLogic, serverLogic,
      paramPartitioner,
      wInPartition,
      workerParallelism,
      psParallelism,
      new SimpleWorkerReceiver[Param](),
      new SimpleWorkerSender[Param](),
      new SimplePSReceiver[Param](),
      new SimplePSSender[Param](),
      iterationWaitTime)

    modelUpdates
  }

  def rangePartitionerPS[P](featureCount: Int)(psParallelism: Int): (WorkerToPS[P]) => Int = {
    val partitionSize = Math.ceil(featureCount.toDouble / psParallelism).toInt
    val partitonerFunction = (paramId: Int) => Math.abs(paramId) / partitionSize

    val paramPartitioner: WorkerToPS[P] => Int = {
      case WorkerToPS(partitionId, msg) => msg match {
        case Left(Pull(paramId)) => partitonerFunction(paramId)
        case Right(Push(paramId, delta)) => partitonerFunction(paramId)
      }
    }

    paramPartitioner
  }

  /**
    * Generic model builder for binary and multiclass cases.
    *
    * @tparam Param
    * Type of Parameter Server parameter.
    * @tparam Model
    * Type of the model.
    */
  private trait ModelBuilder[Param, Model] extends Serializable {

    /**
      * Creates a model out of single parameters.
      *
      * @param params
      * Parameters.
      * @param featureCount
      * Number of features.
      * @return
      * Model.
      */
    def buildModel(params: Iterable[(Int, Param)], featureCount: Int): Model

  }

}

