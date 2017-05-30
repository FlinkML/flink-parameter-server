package hu.sztaki.ilab.ps

import hu.sztaki.ilab.ps.client.receiver.SimpleWorkerReceiver
import hu.sztaki.ilab.ps.client.sender.SimpleWorkerSender
import hu.sztaki.ilab.ps.entities._
import hu.sztaki.ilab.ps.server.SimplePSLogic
import hu.sztaki.ilab.ps.server.receiver.SimplePSReceiver
import hu.sztaki.ilab.ps.server.sender.SimplePSSender
import org.apache.flink.api.common.functions.{Partitioner, RichFlatMapFunction, RuntimeContext}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

class FlinkParameterServer

object FlinkParameterServer {

  private val log = LoggerFactory.getLogger(classOf[FlinkParameterServer])

  /**
    * Applies a transformation to a [[DataStream]] that involves training with a ParameterServer.
    *
    * The ParameterServer logic simply stores parameters in a HashMap and emits the current values of
    * the parameter at every update (see [[SimplePSLogic]]).
    * However, the update mechanism and the parameter initialization should be defined.
    * A parameter is initialized at its first pull, so there must be no pushes to a parameter before
    * it got pulled first.
    *
    * Parameters are partitioned by the hash of their id.
    *
    * @param trainingData
    * [[DataStream]] containing the training data.
    * @param workerLogic
    * Logic of the worker that uses the ParameterServer for training.
    * @param paramInit
    * Function for initializing the parameters based on their id.
    * @param paramUpdate
    * Function for updating the parameters. Takes the old parameter value and a delta update.
    * @param workerParallelism
    * Number of parallel worker instances.
    * @param psParallelism
    * Number of parallel PS instances.
    * @param iterationWaitTime
    * Time to wait for new messages at worker. If set to 0, the job will run infinitely.
    * PS is implemented with a Flink iteration, and Flink does not know when the iteration finishes,
    * so this is how the job can finish.
    * @tparam T
    * Type of training data.
    * @tparam P
    * Type of parameter.
    * @tparam WOut
    * Type of output of workers.
    * @return
    * Transform [[DataStream]] consisting of the worker and PS output.
    */
  def parameterServerTransform[T, P, WOut](trainingData: DataStream[T],
                                           workerLogic: WorkerLogic[T, P, WOut],
                                           paramInit: => Int => P,
                                           paramUpdate: => (P, P) => P,
                                           workerParallelism: Int,
                                           psParallelism: Int,
                                           iterationWaitTime: Long
                                          )
                                          (implicit
                                           tiT: TypeInformation[T],
                                           tiP: TypeInformation[P],
                                           tiWOut: TypeInformation[WOut]
                                          ): DataStream[Either[WOut, (Int, P)]] = {
    val psLogic = new SimplePSLogic[P](paramInit, paramUpdate)
    parameterServerTransform(trainingData, workerLogic, psLogic,
      workerParallelism, psParallelism, iterationWaitTime)
  }

  /**
    * Applies a transformation to a [[DataStream]] that involves training with a ParameterServer.
    * Parameters are partitioned by the hash of their id.
    *
    * @param trainingData
    * [[DataStream]] containing the training data.
    * @param workerLogic
    * Logic of the worker that uses the ParameterServer for training.
    * @param psLogic
    * Logic of the ParameterServer that serves pulls and handles pushes.
    * @param workerParallelism
    * Number of parallel worker instances.
    * @param psParallelism
    * Number of parallel PS instances.
    * @param iterationWaitTime
    * Time to wait for new messages at worker. If set to 0, the job will run infinitely.
    * PS is implemented with a Flink iteration, and Flink does not know when the iteration finishes,
    * so this is how the job can finish.
    * @tparam T
    * Type of training data.
    * @tparam P
    * Type of parameter.
    * @tparam PSOut
    * Type of output of PS.
    * @tparam WOut
    * Type of output of workers.
    * @return
    * Transform [[DataStream]] consisting of the worker and PS output.
    */
  def parameterServerTransform[T, P, PSOut, WOut](trainingData: DataStream[T],
                                                  workerLogic: WorkerLogic[T, P, WOut],
                                                  psLogic: ParameterServerLogic[P, PSOut],
                                                  workerParallelism: Int,
                                                  psParallelism: Int,
                                                  iterationWaitTime: Long)
                                                 (implicit
                                                  tiT: TypeInformation[T],
                                                  tiP: TypeInformation[P],
                                                  tiPSOut: TypeInformation[PSOut],
                                                  tiWOut: TypeInformation[WOut]
                                                 ): DataStream[Either[WOut, PSOut]] = {
    import hu.sztaki.ilab.ps.entities._
    import hu.sztaki.ilab.ps.client.receiver.SimpleWorkerReceiver
    import hu.sztaki.ilab.ps.client.sender.SimpleWorkerSender
    import hu.sztaki.ilab.ps.server.receiver.SimplePSReceiver
    import hu.sztaki.ilab.ps.server.sender.SimplePSSender

    val hashFunc: Any => Int = x => Math.abs(x.hashCode())

    val workerToPSPartitioner: WorkerToPS[P] => Int = {
      case WorkerToPS(_, msg) =>
        msg match {
          case Left(Pull(pId)) => hashFunc(pId) % psParallelism
          case Right(Push(pId, _)) => hashFunc(pId) % psParallelism
        }
    }

    val psToWorkerPartitioner: PSToWorker[P] => Int = {
      case PSToWorker(workerPartitionIndex, _) => workerPartitionIndex
    }

    parameterServerTransform[T, P, PSOut, WOut, PSToWorker[P], WorkerToPS[P]](
      trainingData,
      workerLogic, psLogic,
      workerToPSPartitioner, psToWorkerPartitioner,
      workerParallelism, psParallelism,
      new SimpleWorkerReceiver[P], new SimpleWorkerSender[P],
      new SimplePSReceiver[P], new SimplePSSender[P],
      iterationWaitTime
    )
  }

  /**
    * Applies a transformation to a [[DataStream]] that involves training with a ParameterServer.
    *
    * @param trainingData
    * [[DataStream]] containing the training data.
    * @param workerLogic
    * Logic of the worker that uses the ParameterServer for training.
    * @param psLogic
    * Logic of the ParameterServer that serves pulls and handles pushes.
    * @param paramPartitioner
    * Partitioning messages from the worker to PS.
    * @param wInPartition
    * Partitioning messages from the PS to worker.
    * @param workerParallelism
    * Number of parallel worker instances.
    * @param psParallelism
    * Number of parallel PS instances.
    * @param workerReceiver
    * Logic of forming the messages received at worker from PS to a pull answer.
    * @param workerSender
    * Logic of wrapping the pulls and pushes into messages sent by worker to PS.
    * @param psReceiver
    * Logic of forming the messages received at PS from a worker to a pulls and pushes.
    * @param psSender
    * Logic of wrapping the pull answers into messages sent by PS to worker.
    * @param iterationWaitTime
    * Time to wait for new messages at worker. If set to 0, the job will run infinitely.
    * PS is implemented with a Flink iteration, and Flink does not know when the iteration finishes,
    * so this is how the job can finish.
    * @tparam T
    * Type of training data.
    * @tparam P
    * Type of parameter.
    * @tparam PSOut
    * Type of output of PS.
    * @tparam WOut
    * Type of output of workers.
    * @tparam PStoWorker
    * Type of message from PS to workers.
    * @tparam WorkerToPS
    * Type of message from workers to PS.
    * @return
    * Transform [[DataStream]] consisting of the worker and PS output.
    */
  def parameterServerTransform[T, P, PSOut, WOut, PStoWorker, WorkerToPS](trainingData: DataStream[T],
                                                                          workerLogic: WorkerLogic[T, P, WOut],
                                                                          psLogic: ParameterServerLogic[P, PSOut],
                                                                          paramPartitioner: WorkerToPS => Int,
                                                                          wInPartition: PStoWorker => Int,
                                                                          workerParallelism: Int,
                                                                          psParallelism: Int,
                                                                          workerReceiver: WorkerReceiver[PStoWorker, P],
                                                                          workerSender: WorkerSender[WorkerToPS, P],
                                                                          psReceiver: PSReceiver[WorkerToPS, P],
                                                                          psSender: PSSender[PStoWorker, P],
                                                                          iterationWaitTime: Long = 10000)
                                                                         (implicit
                                                                          tiT: TypeInformation[T],
                                                                          tiP: TypeInformation[P],
                                                                          tiPSOut: TypeInformation[PSOut],
                                                                          tiWOut: TypeInformation[WOut],
                                                                          tiWorkerIn: TypeInformation[PStoWorker],
                                                                          tiWorkerOut: TypeInformation[WorkerToPS]
                                                                         ): DataStream[Either[WOut, PSOut]] = {
    def stepFunc(workerIn: ConnectedStreams[T, PStoWorker]):
    (DataStream[PStoWorker], DataStream[Either[WOut, PSOut]]) = {

      val worker = workerIn
        .flatMap(
          new RichCoFlatMapFunction[T, PStoWorker, Either[WorkerToPS, WOut]] {

            val receiver: WorkerReceiver[PStoWorker, P] = workerReceiver
            val sender: WorkerSender[WorkerToPS, P] = workerSender
            val logic: WorkerLogic[T, P, WOut] = workerLogic

            val psClient =
              new MessagingPSClient[PStoWorker, WorkerToPS, P, WOut](sender)


            override def open(parameters: Configuration): Unit = {
              psClient.setPartitionId(getRuntimeContext.getIndexOfThisSubtask)
            }

            // incoming answer from PS
            override def flatMap2(msg: PStoWorker, out: Collector[Either[WorkerToPS, WOut]]): Unit = {
              log.debug(s"Pull answer: $msg")

              psClient.setCollector(out)
              receiver.onPullAnswerRecv(msg, {
                case PullAnswer(id, value) => logic.onPullRecv(id, value, psClient)
              })
            }

            // incoming data
            override def flatMap1(data: T, out: Collector[Either[WorkerToPS, WOut]]): Unit = {
              log.debug(s"Incoming data: $data")

              psClient.setCollector(out)
              logic.onRecv(data, psClient)
            }

            override def close(): Unit = {
              logic.close()
            }
          }
        )
        .setParallelism(workerParallelism)

      val wOut = worker.flatMap(x => x match {
        case Right(out) => Some(out)
        case _ => None
      })

      val ps = worker
        .flatMap(x => x match {
          case Left(workerOut) => Some(workerOut)
          case _ => None
        })
        .partitionCustom(new Partitioner[Int]() {
          override def partition(key: Int, numPartitions: Int): Int = {
            key % numPartitions
          }
        }, paramPartitioner)
        .flatMap(new RichFlatMapFunction[WorkerToPS, Either[PStoWorker, PSOut]] {

          val logic: ParameterServerLogic[P, PSOut] = psLogic
          val receiver: PSReceiver[WorkerToPS, P] = psReceiver
          val sender: PSSender[PStoWorker, P] = psSender

          val ps = new MessagingPS[PStoWorker, WorkerToPS, P, PSOut](sender)

          override def flatMap(msg: WorkerToPS, out: Collector[Either[PStoWorker, PSOut]]): Unit = {
            log.debug(s"Pull request or push msg @ PS: $msg")

            ps.setCollector(out)
            receiver.onWorkerMsg(msg,
              (pullId, workerPartitionIndex) => logic.onPullRecv(pullId, workerPartitionIndex, ps), { case (pushId, deltaUpdate) => logic.onPushRecv(pushId, deltaUpdate, ps) }
            )
          }

          override def close(): Unit = {
            logic.close(ps)
          }

          override def open(parameters: Configuration): Unit =
            logic.open(parameters: Configuration, getRuntimeContext: RuntimeContext)
        })
        .setParallelism(psParallelism)

      val psToWorker = ps
        .flatMap(_ match {
          case Left(x) => Some(x)
          case _ => None
        })
        .setParallelism(psParallelism)
        // TODO avoid this empty map?
        .map(x => x).setParallelism(workerParallelism)
        .partitionCustom(new Partitioner[Int]() {
          override def partition(key: Int, numPartitions: Int): Int = {
            if (0 <= key && key < numPartitions) {
              key
            } else {
              throw new RuntimeException("Pull answer key should be the partition ID itself!")
            }
          }
        }, wInPartition)

      val psToOut = ps.flatMap(_ match {
        case Right(x) => Some(x)
        case _ => None
      })
        .setParallelism(psParallelism)

      val wOutEither: DataStream[Either[WOut, PSOut]] = wOut.map(x => Left(x))
      val psOutEither: DataStream[Either[WOut, PSOut]] = psToOut.map(x => Right(x))

      (psToWorker, wOutEither.union(psOutEither))
    }

    trainingData
      .map(x => x)
      .setParallelism(workerParallelism)
      .iterate((x: ConnectedStreams[T, PStoWorker]) => stepFunc(x), iterationWaitTime)
  }

  /**
    * Applies a transformation to a [[org.apache.flink.streaming.api.scala.DataStream]] that uses a ParameterServer.
    * Initial parameters can be loaded by a [[org.apache.flink.streaming.api.scala.DataStream]].
    *
    * NOTE:
    * ParameterServerLogic must accept push messages before pulls,
    * and in WorkerLogic a parameter should be pulled before pushed.
    *
    * @param model
    * Initial parameters to load.
    * @param trainingData
    * [[org.apache.flink.streaming.api.scala.DataStream]] containing the training data.
    * @param workerLogic
    * Logic of the worker that uses the ParameterServer for training.
    * @param psLogic
    * Logic of the ParameterServer that serves pulls and handles pushes.
    * @param paramPartitioner
    * Partitioning messages from the worker to PS.
    * @param wInPartition
    * Partitioning messages from the PS to worker.
    * @param workerParallelism
    * Number of parallel worker instances.
    * @param psParallelism
    * Number of parallel PS instances.
    * @param iterationWaitTime
    * Time to wait for new messages at worker. If set to 0, the job will run infinitely.
    * PS is implemented with a Flink iteration, and Flink does not know when the iteration finishes,
    * so this is how the job can finish.
    * @tparam T
    * Type of training data.
    * @tparam P
    * Type of parameter.
    * @tparam PSOut
    * Type of output of PS.
    * @tparam WOut
    * Type of output of workers.
    * @return
    * Transform [[DataStream]] consisting of the worker and PS output.
    */
  def transformWithModelLoad[T, P, PSOut, WOut](model: DataStream[(Int, P)])
                                               (trainingData: DataStream[T],
                                                workerLogic: WorkerLogic[T, P, WOut],
                                                psLogic: ParameterServerLogic[P, PSOut],
                                                paramPartitioner: WorkerToPS[P] => Int,
                                                wInPartition: PSToWorker[P] => Int,
                                                workerParallelism: Int,
                                                psParallelism: Int,
                                                iterationWaitTime: Long = 10000)
                                               (implicit
                                                tiT: TypeInformation[T],
                                                tiP: TypeInformation[P],
                                                tiPSOut: TypeInformation[PSOut],
                                                tiWOut: TypeInformation[WOut]
                                               ): DataStream[Either[WOut, PSOut]] = {


    case class EOF() extends Serializable

    type ModelOrT = Either[Either[EOF, (Int, P)], T]

    val modelWithEOF: DataStream[ModelOrT] =
      model.rebalance.map(x => x).setParallelism(workerParallelism)
        .forward.flatMap(new RichFlatMapFunction[(Int, P), ModelOrT] {

        var collector: Collector[ModelOrT] = _

        override def flatMap(value: (Int, P), out: Collector[ModelOrT]): Unit = {
          if (collector == null) {
            collector = out
          }
          out.collect(Left(Right(value)))
        }

        override def close(): Unit = {
          if (collector != null) {
            collector.collect(Left(Left(EOF())))
          } else {
            throw new IllegalStateException("There must be a parameter per model partition when loading model.")
          }
        }
      }).setParallelism(workerParallelism)

    val trainingDataPrepared: DataStream[ModelOrT] = trainingData.map(Right(_))

    // TODO do not wrap PSClient every time it's used
    def wrapPSClient(ps: ParameterServerClient[Either[EOF, P], WOut]): ParameterServerClient[P, WOut] =
      new ParameterServerClient[P, WOut] {
        override def pull(id: Int): Unit = ps.pull(id)

        override def push(id: Int, deltaUpdate: P): Unit = ps.push(id, Right(deltaUpdate))

        override def output(out: WOut): Unit = ps.output(out)
      }

    val wrappedWorkerLogic = new WorkerLogic[ModelOrT, Either[EOF, P], WOut] {

      var receivedEOF = false
      val dataBuffer = new ArrayBuffer[T]()

      override def onRecv(modelOrDataPoint: ModelOrT, ps: ParameterServerClient[Either[EOF, P], WOut]): Unit = {
        modelOrDataPoint match {
          case Left(param) =>
            param match {
              case Left(EOF()) =>
                receivedEOF = true

                // notify all PS instance
                (0 until psParallelism).foreach {
                  psIdx => ps.push(psIdx, Left(EOF()))
                }

                // process buffered data
                val wrappedPS = wrapPSClient(ps)
                dataBuffer.foreach {
                  dataPoint => workerLogic.onRecv(dataPoint, wrappedPS)
                }
              case Right((paramId, paramValue)) => ps.push(paramId, Right(paramValue))
            }
          case Right(dataPoint) =>
            if (receivedEOF) {
              workerLogic.onRecv(dataPoint, wrapPSClient(ps))
            } else {
              dataBuffer.append(dataPoint)
            }
        }
      }

      override def onPullRecv(paramId: Int,
                              paramValue: Either[EOF, P],
                              ps: ParameterServerClient[Either[EOF, P], WOut]): Unit = {
        paramValue match {
          case Right(p) =>
            workerLogic.onPullRecv(paramId, p, wrapPSClient(ps))
          case _ =>
            throw new IllegalStateException("PS should not send EOF pull answers")
        }
      }

      override def close(): Unit = {
        workerLogic.close()
      }
    }

    val wrappedParamPartitioner: WorkerToPS[Either[EOF, P]] => Int = {
      case WorkerToPS(workerPartitionIndex, msg) => msg match {
        case pull@Left(Pull(paramId)) =>
          paramPartitioner(WorkerToPS(workerPartitionIndex, pull.asInstanceOf[Either[Pull, Push[P]]]))
        case pushMsg@Right(Push(paramId, deltaOrEOF)) => deltaOrEOF match {
          case Left(EOF()) => paramId
          case Right(delta) => paramPartitioner(WorkerToPS(workerPartitionIndex, Right(Push(paramId, delta))))
        }
      }
    }

    // TODO do not wrap PS every time it's used
    def wrapPS(ps: ParameterServer[Either[EOF, P], PSOut]): ParameterServer[P, PSOut] =
      new ParameterServer[P, PSOut] {

        override def answerPull(id: Int, value: P, workerPartitionIndex: Int): Unit =
          ps.answerPull(id, Right(value), workerPartitionIndex)

        override def output(out: PSOut): Unit =
          ps.output(out)
      }

    val wrappedPSLogic = new ParameterServerLogic[Either[EOF, P], PSOut] {

      var eofCountDown: Int = workerParallelism

      val pullBuffer = new ArrayBuffer[(Int, Int)]()

      override def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[Either[EOF, P], PSOut]): Unit = {
        if (eofCountDown == 0) {
          psLogic.onPullRecv(id, workerPartitionIndex, wrapPS(ps))
        } else {
          pullBuffer.append((id, workerPartitionIndex))
        }
      }

      override def onPushRecv(id: Int,
                              deltaUpdate: Either[EOF, P],
                              ps: ParameterServer[Either[EOF, P], PSOut]): Unit = {
        deltaUpdate match {
          case Left(EOF()) =>
            eofCountDown -= 1

            if (eofCountDown == 0) {
              // we have received the model, we can process the buffered pulls
              pullBuffer.foreach {
                case (paramId, workerPartitionIndex) =>
                  psLogic.onPullRecv(paramId, workerPartitionIndex, wrapPS(ps))
              }
            }
          case Right(param) =>
            psLogic.onPushRecv(id, param, wrapPS(ps))
        }

      }

      override def close(ps: ParameterServer[Either[EOF, P], PSOut]): Unit =
        psLogic.close(wrapPS(ps))

      override def open(parameters: Configuration, runtimeContext: RuntimeContext): Unit =
        psLogic.open(parameters, runtimeContext)
    }

    val wrappedWorkerInPartition: PSToWorker[Either[EOF, P]] => Int = {
      case PSToWorker(workerPartitionIndex, PullAnswer(paramId, Right(param))) =>
        wInPartition(PSToWorker(workerPartitionIndex, PullAnswer(paramId, param)))
    }

    parameterServerTransform(
      modelWithEOF.union(trainingDataPrepared),
      wrappedWorkerLogic,
      wrappedPSLogic,
      wrappedParamPartitioner,
      wrappedWorkerInPartition,
      workerParallelism,
      psParallelism,
      new SimpleWorkerReceiver[Either[EOF, P]],
      new SimpleWorkerSender[Either[EOF, P]],
      new SimplePSReceiver[Either[EOF, P]],
      new SimplePSSender[Either[EOF, P]]
    )
  }

  /**
    * Connects [[ParameterServer]] and [[PSSender]] to Flink logic.
    */
  private class MessagingPS[WorkerIn, WorkerOut, P, PSOut](psSender: PSSender[WorkerIn, P])
    extends ParameterServer[P, PSOut] {

    private var collector: Collector[Either[WorkerIn, PSOut]] = _

    def setCollector(out: Collector[Either[WorkerIn, PSOut]]): Unit = {
      collector = out
    }

    def collectAnswerMsg(msg: WorkerIn): Unit = {
      collector.collect(Left(msg))
    }

    override def answerPull(id: Int, value: P, workerPartitionIndex: Int): Unit = {
      psSender.onPullAnswer(id, value, workerPartitionIndex, collectAnswerMsg)
    }

    override def output(out: PSOut): Unit = {
      collector.collect(Right(out))
    }
  }

  /**
    * Connects [[ParameterServerClient]] and [[WorkerSender]] to Flink logic.
    */
  private class MessagingPSClient[IN, OUT, P, WOut](sender: WorkerSender[OUT, P])
    extends ParameterServerClient[P, WOut] {

    private var collector: Collector[Either[OUT, WOut]] = _
    private var partitionId: Int = -1

    def setPartitionId(pId: Int): Unit = {
      partitionId = pId
    }

    def setCollector(out: Collector[Either[OUT, WOut]]): Unit = {
      collector = out
    }

    def collectPullMsg(msg: OUT): Unit = {
      collector.collect(Left(msg))
    }

    override def pull(id: Int): Unit =
      sender.onPull(id, collectPullMsg, partitionId)

    override def push(id: Int, deltaUpdate: P): Unit =
      sender.onPush(id, deltaUpdate, collectPullMsg, partitionId)

    override def output(out: WOut): Unit = {
      collector.collect(Right(out))
    }
  }

}


/**
  * Logic of the ParameterServer, that stores the parameters,
  * applies pushes and answers pulls.
  *
  * This could be implemented if needed (e.g. for controlling the output of parameters),
  * but it is not necessary to have a custom implementation.
  * By default a [[hu.sztaki.ilab.ps.server.SimplePSLogic]] is used.
  *
  * @tparam P
  * Type of parameters.
  * @tparam PSOut
  * Type of ParameterServer output.
  */
trait ParameterServerLogic[P, PSOut] extends Serializable {

  /**
    * Method called when a pull message arrives from a worker.
    *
    * @param id
    * Identifier of parameter (e.g. it could be an index of a vector).
    * @param workerPartitionIndex
    * Index of the worker partition.
    * @param ps
    * Interface for answering pulls and creating output.
    */
  def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[P, PSOut]): Unit

  /**
    * Method called when a push message arrives from a worker.
    *
    * @param id
    * Identifier of parameter (e.g. it could be an index of a vector).
    * @param deltaUpdate
    * Value to update the parameter (e.g. it could be added to the current value).
    * @param ps
    * Interface for answering pulls and creating output.
    */
  def onPushRecv(id: Int, deltaUpdate: P, ps: ParameterServer[P, PSOut]): Unit

  /**
    * Method called when processing is finished.
    */
  def close(ps: ParameterServer[P, PSOut]): Unit = ()

  /**
    * Method called when the class is initialized.
    */
  def open(parameters: Configuration, runtimeContext: RuntimeContext): Unit = ()
}

trait ParameterServer[P, PSOut] extends Serializable {
  def answerPull(id: Int, value: P, workerPartitionIndex: Int): Unit

  def output(out: PSOut): Unit
}

/**
  * Logic of forming the messages received at PS from a worker to a pulls and pushes.
  *
  * @tparam WorkerToPS
  * Type of message from workers to PS.
  * @tparam P
  * Type of parameter.
  */
trait PSReceiver[WorkerToPS, P] extends Serializable {
  def onWorkerMsg(msg: WorkerToPS,
                  onPullRecv: (Int, Int) => Unit,
                  onPushRecv: (Int, P) => Unit)
}

/**
  * Logic of wrapping the pull answers into messages sent by PS to worker.
  *
  * @tparam PStoWorker
  * Type of message from PS to workers.
  * @tparam P
  * Type of parameter.
  */
trait PSSender[PStoWorker, P] extends Serializable {
  def onPullAnswer(id: Int,
                   value: P,
                   workerPartitionIndex: Int,
                   collectAnswerMsg: PStoWorker => Unit)
}

/**
  * Logic of forming the messages received at worker from PS to a pull answer.
  *
  * @tparam PStoWorker
  * Type of message from PS to workers.
  * @tparam P
  * Type of parameter.
  */
trait WorkerReceiver[PStoWorker, P] extends Serializable {
  def onPullAnswerRecv(msg: PStoWorker, pullHandler: PullAnswer[P] => Unit)
}

/**
  * Logic of wrapping the pulls and pushes into messages sent by worker to PS.
  *
  * @tparam WorkerToPS
  * Type of message from workers to PS.
  * @tparam P
  * Type of parameter.
  */
trait WorkerSender[WorkerToPS, P] extends Serializable {
  def onPull(id: Int, collectAnswerMsg: WorkerToPS => Unit, partitionId: Int)

  def onPush(id: Int, deltaUpdate: P, collectAnswerMsg: WorkerToPS => Unit, partitionId: Int)
}

