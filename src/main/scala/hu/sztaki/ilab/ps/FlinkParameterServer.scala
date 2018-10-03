package hu.sztaki.ilab.ps

import hu.sztaki.ilab.ps.client.receiver.SimpleWorkerReceiver
import hu.sztaki.ilab.ps.client.sender.SimpleWorkerSender
import hu.sztaki.ilab.ps.entities._
import hu.sztaki.ilab.ps.matrix.factorization.workers.BaseMFWorkerLogic
import hu.sztaki.ilab.ps.server.{LooseSimplePSLogic, SimplePSLogic}
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
    * However, the update mechanism ands the parameter initialization should be defined.
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
    * @tparam Id
    * Type of parameter identifier.
    * @tparam P
    * Type of parameter.
    * @tparam WOut
    * Type of output of workers.
    * @return
    * Transform [[DataStream]] consisting of the worker and PS output.
    */
  def transform[T, Id, P, WOut](trainingData: DataStream[T],
                            workerLogic: WorkerLogic[T, Id, P, WOut],
                            paramInit: => Id => P,
                            paramUpdate: => (P, P) => P,
                            workerParallelism: Int,
                            psParallelism: Int,
                            iterationWaitTime: Long)
                           (implicit
                            tiT: TypeInformation[T],
                            tiId: TypeInformation[Id],
                            tiP: TypeInformation[P],
                            tiWOut: TypeInformation[WOut]
                           ): DataStream[Either[WOut, (Id, P)]] = {
    val psLogic = new SimplePSLogic[Id, P](paramInit, paramUpdate)
    transform(trainingData, workerLogic, psLogic,
      workerParallelism, psParallelism, iterationWaitTime)
  }

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
    * @tparam Id
    * Type of parameter identifier.
    * @tparam PullP
    * Type of Pull parameter.
    * @tparam PushP
    * Type of Push parameter.
    * @tparam WOut
    * Type of output of workers.
    * @return
    * Transform [[DataStream]] consisting of the worker and PS output.
    */
  def transform[T, Id, PullP, PushP, WOut](trainingData: DataStream[T],
                            workerLogic: LooseWorkerLogic[T, Id, PullP, PushP, WOut],
                            paramInit: => Id => PullP,
                            paramUpdate: => (PullP, PushP) => PullP,
                            workerParallelism: Int,
                            psParallelism: Int,
                            iterationWaitTime: Long)
                           (implicit
                            tiT: TypeInformation[T],
                            tiId: TypeInformation[Id],
                            tiPull: TypeInformation[PullP],
                            tiPush: TypeInformation[PushP],
                            tiWOut: TypeInformation[WOut]
                           ): DataStream[Either[WOut, (Id, PullP)]] = {
    val psLogic = new LooseSimplePSLogic[Id, PullP, PushP](paramInit, paramUpdate)
    transform(trainingData, workerLogic, psLogic,
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
    * @tparam Id
    * Type of parameter identifier.
    * @tparam P
    * Type of parameter.
    * @tparam PSOut
    * Type of output of PS.
    * @tparam WOut
    * Type of output of workers.
    * @return
    * Transform [[DataStream]] consisting of the worker and PS output.
    */
  def transform[T, Id, P, PSOut, WOut](trainingData: DataStream[T],
                                   workerLogic: WorkerLogic[T, Id, P, WOut],
                                   psLogic: ParameterServerLogic[Id, P, PSOut],
                                   workerParallelism: Int,
                                   psParallelism: Int,
                                   iterationWaitTime: Long)
                                  (implicit
                                   tiT: TypeInformation[T],
                                   tiId: TypeInformation[Id],
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

    val workerToPSPartitioner: WorkerToPS[Id, P] => Int = {
      case WorkerToPS(_, msg) =>
        msg match {
          case Left(Pull(pId)) => hashFunc(pId) % psParallelism
          case Right(Push(pId, _)) => hashFunc(pId) % psParallelism
        }
    }

    val psToWorkerPartitioner: PSToWorker[Id, P] => Int = {
      case PSToWorker(workerPartitionIndex, _) => workerPartitionIndex
    }

    transform[T, Id, P, PSOut, WOut, PSToWorker[Id, P], WorkerToPS[Id, P]](
      trainingData,
      workerLogic, psLogic,
      workerToPSPartitioner, psToWorkerPartitioner,
      workerParallelism, psParallelism,
      new SimpleWorkerReceiver[Id, P], new SimpleWorkerSender[Id, P],
      new SimplePSReceiver[Id, P], new SimplePSSender[Id, P],
      iterationWaitTime
    )
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
    * @tparam Id
    * Type of parameter identifier.
    * @tparam PullP
    * Type of Pull parameter.
    * @tparam PushP
    * Type of Push parameter.
    * @tparam PSOut
    * Type of output of PS.
    * @tparam WOut
    * Type of output of workers.
    * @return
    * Transform [[DataStream]] consisting of the worker and PS output.
    */
  def transform[T, Id, PullP, PushP, PSOut, WOut](trainingData: DataStream[T],
                                   workerLogic: LooseWorkerLogic[T, Id, PullP, PushP, WOut],
                                   psLogic: LooseParameterServerLogic[Id, PullP, PushP, PSOut],
                                   workerParallelism: Int,
                                   psParallelism: Int,
                                   iterationWaitTime: Long)
                                  (implicit
                                   tiT: TypeInformation[T],
                                   tiId: TypeInformation[Id],
                                   tiPull: TypeInformation[PullP],
                                   tiPush: TypeInformation[PushP],
                                   tiPSOut: TypeInformation[PSOut],
                                   tiWOut: TypeInformation[WOut]
                                  ): DataStream[Either[WOut, PSOut]] = {
    import hu.sztaki.ilab.ps.entities._
    import hu.sztaki.ilab.ps.client.receiver.SimpleWorkerReceiver
    import hu.sztaki.ilab.ps.client.sender.SimpleWorkerSender
    import hu.sztaki.ilab.ps.server.receiver.SimplePSReceiver
    import hu.sztaki.ilab.ps.server.sender.SimplePSSender

    val hashFunc: Any => Int = x => Math.abs(x.hashCode())

    val workerToPSPartitioner: WorkerToPS[Id, PushP] => Int = {
      case WorkerToPS(_, msg) =>
        msg match {
          case Left(Pull(pId)) => hashFunc(pId) % psParallelism
          case Right(Push(pId, _)) => hashFunc(pId) % psParallelism
        }
    }

    val psToWorkerPartitioner: PSToWorker[Id, PullP] => Int = {
      case PSToWorker(workerPartitionIndex, _) => workerPartitionIndex
    }

    transform[T, Id, PullP, PushP, PSOut, WOut, PSToWorker[Id, PullP], WorkerToPS[Id, PushP]](
      trainingData,
      workerLogic, psLogic,
      workerToPSPartitioner, psToWorkerPartitioner,
      workerParallelism, psParallelism,
      new SimpleWorkerReceiver[Id, PullP], new SimpleWorkerSender[Id, PushP],
      new SimplePSReceiver[Id, PushP], new SimplePSSender[Id, PullP],
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
    * @tparam Id
    * Type of parameter identifier.
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
  def transform[T, Id, P, PSOut, WOut, PStoWorker, WorkerToPS](trainingData: DataStream[T],
                                                           workerLogic: WorkerLogic[T, Id, P, WOut],
                                                           psLogic: ParameterServerLogic[Id, P, PSOut],
                                                           paramPartitioner: WorkerToPS => Int,
                                                           wInPartition: PStoWorker => Int,
                                                           workerParallelism: Int,
                                                           psParallelism: Int,
                                                           workerReceiver: WorkerReceiver[PStoWorker, Id, P],
                                                           workerSender: WorkerSender[WorkerToPS, Id, P],
                                                           psReceiver: PSReceiver[WorkerToPS, Id, P],
                                                           psSender: PSSender[PStoWorker, Id, P],
                                                           iterationWaitTime: Long)
                                                          (implicit
                                                           tiT: TypeInformation[T],
                                                           tiId: TypeInformation[Id],
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

            val receiver: WorkerReceiver[PStoWorker, Id, P] = workerReceiver
            val sender: WorkerSender[WorkerToPS, Id, P] = workerSender
            val logic: WorkerLogic[T, Id, P, WOut] = workerLogic

            val psClient =
              new MessagingPSClient[PStoWorker, WorkerToPS, Id, P, WOut](sender)


            override def open(parameters: Configuration): Unit = {
              logic.open()
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
      }).setParallelism(workerParallelism)

      val ps = worker
        .flatMap(x => x match {
          case Left(workerOut) => Some(workerOut)
          case _ => None
        }).setParallelism(workerParallelism)
        .partitionCustom(new Partitioner[Int]() {
          override def partition(key: Int, numPartitions: Int): Int = {
            key % numPartitions
          }
        }, paramPartitioner)
        .flatMap(new RichFlatMapFunction[WorkerToPS, Either[PStoWorker, PSOut]] {

          val logic: ParameterServerLogic[Id, P, PSOut] = psLogic
          val receiver: PSReceiver[WorkerToPS, Id, P] = psReceiver
          val sender: PSSender[PStoWorker, Id, P] = psSender

          val ps = new MessagingPS[PStoWorker, WorkerToPS, Id, P, PSOut](sender)

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

      val wOutEither: DataStream[Either[WOut, PSOut]] = wOut.forward.map(x => Left(x))
      val psOutEither: DataStream[Either[WOut, PSOut]] = psToOut.forward.map(x => Right(x))

      (psToWorker, wOutEither.setParallelism(workerParallelism).union(psOutEither.setParallelism(psParallelism)))
    }

    trainingData
      .map(x => x)
      .setParallelism(workerParallelism)
      .iterate((x: ConnectedStreams[T, PStoWorker]) => stepFunc(x), iterationWaitTime)
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
    * @tparam Id
    * Type of parameter identifier.
    * @tparam PullP
    * Type of Pull parameter.
    * @tparam PushP
    * Type of Push parameter.
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
  def transform[T, Id, PullP, PushP, PSOut, WOut, PStoWorker, WorkerToPS](trainingData: DataStream[T],
                                                           workerLogic: LooseWorkerLogic[T, Id, PullP, PushP, WOut],
                                                           psLogic: LooseParameterServerLogic[Id, PullP, PushP, PSOut],
                                                           paramPartitioner: WorkerToPS => Int,
                                                           wInPartition: PStoWorker => Int,
                                                           workerParallelism: Int,
                                                           psParallelism: Int,
                                                           workerReceiver: WorkerReceiver[PStoWorker, Id, PullP],
                                                           workerSender: WorkerSender[WorkerToPS, Id, PushP],
                                                           psReceiver: PSReceiver[WorkerToPS, Id, PushP],
                                                           psSender: PSSender[PStoWorker, Id, PullP],
                                                           iterationWaitTime: Long)
                                                          (implicit
                                                           tiT: TypeInformation[T],
                                                           tiId: TypeInformation[Id],
                                                           tiPullP: TypeInformation[PullP],
                                                           tiPushP: TypeInformation[PushP],
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

            val receiver: WorkerReceiver[PStoWorker, Id, PullP] = workerReceiver
            val sender: WorkerSender[WorkerToPS, Id, PushP] = workerSender
            val logic: LooseWorkerLogic[T, Id, PullP, PushP, WOut] = workerLogic

            val psClient =
              new MessagingPSClient[PStoWorker, WorkerToPS, Id, PushP, WOut](sender)


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
      }).setParallelism(workerParallelism)

      val ps = worker
        .flatMap(x => x match {
          case Left(workerOut) => Some(workerOut)
          case _ => None
        }).setParallelism(workerParallelism)
        .partitionCustom(new Partitioner[Int]() {
          override def partition(key: Int, numPartitions: Int): Int = {
            key % numPartitions
          }
        }, paramPartitioner)
        .flatMap(new RichFlatMapFunction[WorkerToPS, Either[PStoWorker, PSOut]] {

          val logic: LooseParameterServerLogic[Id, PullP, PushP, PSOut] = psLogic
          val receiver: PSReceiver[WorkerToPS, Id, PushP] = psReceiver
          val sender: PSSender[PStoWorker, Id, PullP] = psSender

          val ps = new MessagingPS[PStoWorker, WorkerToPS, Id, PullP, PSOut](sender)

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

      val wOutEither: DataStream[Either[WOut, PSOut]] = wOut.forward.map(x => Left(x))
      val psOutEither: DataStream[Either[WOut, PSOut]] = psToOut.forward.map(x => Right(x))

      (psToWorker, wOutEither.setParallelism(workerParallelism).union(psOutEither.setParallelism(psParallelism)))
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
    * @tparam Id
    * Type of parameter identifier.
    * @tparam P
    * Type of parameter.
    * @tparam PSOut
    * Type of output of PS.
    * @tparam WOut
    * Type of output of workers.
    * @return
    * Transform [[DataStream]] consisting of the worker and PS output.
    */
  def transformWithModelLoad[T, Id, P, PSOut, WOut](model: DataStream[(Id, P)])
                                               (trainingData: DataStream[T],
                                                workerLogic: WorkerLogic[T, Id, P, WOut],
                                                psLogic: ParameterServerLogic[Id, P, PSOut],
                                                paramPartitioner: WorkerToPS[Id, P] => Int,
                                                wInPartition: PSToWorker[Id, P] => Int,
                                                workerParallelism: Int,
                                                psParallelism: Int,
                                                iterationWaitTime: Long)
                                               (implicit
                                                tiT: TypeInformation[T],
                                                tiId: TypeInformation[Id],
                                                tiP: TypeInformation[P],
                                                tiPSOut: TypeInformation[PSOut],
                                                tiWOut: TypeInformation[WOut]
                                               ): DataStream[Either[WOut, PSOut]] = {


    case class EOF() extends Serializable

    type IntOrId = Either[Int, Id]
    type EOFOrP = Either[EOF, P]
    type ModelOrT = Either[Either[EOF, (Id, P)], T]

    val modelWithEOF: DataStream[ModelOrT] =
      model
        .rebalance.map(x => x)
        .setParallelism(workerParallelism)
        .forward
        .flatMap(new RichFlatMapFunction[(Id, P), ModelOrT] {

        var collector: Collector[ModelOrT] = _

        override def flatMap(value: (Id, P), out: Collector[ModelOrT]): Unit = {
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

    val trainingDataPrepared: DataStream[ModelOrT] = trainingData.rebalance.map(x => x).setParallelism(workerParallelism)
      .forward.map(Right(_))

    // TODO do not wrap PSClient every time it's used
    def wrapPSClient(ps: ParameterServerClient[IntOrId, EOFOrP, WOut]): ParameterServerClient[Id, P, WOut] =
      new ParameterServerClient[Id, P, WOut] {
        override def pull(id: Id): Unit = ps.pull(Right(id))

        override def push(id: Id, deltaUpdate: P): Unit = ps.push(Right(id), Right(deltaUpdate))

        override def output(out: WOut): Unit = ps.output(out)
      }

    val wrappedWorkerLogic = new WorkerLogic[ModelOrT, IntOrId, EOFOrP, WOut] {

      var receivedEOF = false
      val dataBuffer = new ArrayBuffer[T]()

      override def onRecv(modelOrDataPoint: ModelOrT, ps: ParameterServerClient[IntOrId, EOFOrP, WOut]): Unit = {
        modelOrDataPoint match {
          case Left(param) =>
            param match {
              case Left(EOF()) =>
                receivedEOF = true

                // notify all PS instance
                (0 until psParallelism).foreach {
                  psIdx => ps.push(Left(psIdx), Left(EOF()))
                }

                // process buffered data
                val wrappedPS = wrapPSClient(ps)
                dataBuffer.foreach {
                  dataPoint => workerLogic.onRecv(dataPoint, wrappedPS)
                }
              case Right((paramId, paramValue)) => ps.push(Right(paramId), Right(paramValue))
            }
          case Right(dataPoint) =>
            if (receivedEOF) {
              workerLogic.onRecv(dataPoint, wrapPSClient(ps))
            } else {
              dataBuffer.append(dataPoint)
            }
        }
      }

      override def onPullRecv(paramId: IntOrId,
                              paramValue: EOFOrP,
                              ps: ParameterServerClient[IntOrId, EOFOrP, WOut]): Unit = {
        paramValue match {
          case Right(p) =>
            workerLogic.onPullRecv(paramId.right.get, p, wrapPSClient(ps))
          case _ =>
            throw new IllegalStateException("PS should not send EOF pull answers")
        }
      }

      override def close(): Unit = {
        workerLogic.close()
      }
    }

    val wrappedParamPartitioner: WorkerToPS[IntOrId, EOFOrP] => Int = {
      case WorkerToPS(workerPartitionIndex, msg) => msg match {
        case Left(Pull(paramId)) =>
          paramPartitioner(WorkerToPS(workerPartitionIndex, Left(Pull(paramId.right.get))))
        case Right(Push(paramId, deltaOrEOF)) => deltaOrEOF match {
          case Left(EOF()) => paramId.left.get
          case Right(delta) => paramPartitioner(WorkerToPS(workerPartitionIndex, Right(Push(paramId.right.get, delta))))
        }
      }
    }

    // TODO do not wrap PS every time it's used
    def wrapPS(ps: ParameterServer[IntOrId, EOFOrP, PSOut]): ParameterServer[Id, P, PSOut] =
      new ParameterServer[Id, P, PSOut] {

        override def answerPull(id: Id, value: P, workerPartitionIndex: Int): Unit =
          ps.answerPull(Right(id), Right(value), workerPartitionIndex)

        override def output(out: PSOut): Unit =
          ps.output(out)
      }

    val wrappedPSLogic = new ParameterServerLogic[IntOrId, EOFOrP, PSOut] {

      var eofCountDown: Int = workerParallelism

      val pullBuffer = new ArrayBuffer[(Id, Int)]()

      override def onPullRecv(id: IntOrId, workerPartitionIndex: Int, ps: ParameterServer[IntOrId, EOFOrP, PSOut]): Unit = {
        if (eofCountDown == 0) {
          psLogic.onPullRecv(id.right.get, workerPartitionIndex, wrapPS(ps))
        } else {
          pullBuffer.append((id.right.get, workerPartitionIndex))
        }
      }

      override def onPushRecv(id: IntOrId,
                              deltaUpdate: EOFOrP,
                              ps: ParameterServer[IntOrId, EOFOrP, PSOut]): Unit = {
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
            psLogic.onPushRecv(id.right.get, param, wrapPS(ps))
        }

      }

      override def close(ps: ParameterServer[IntOrId, EOFOrP, PSOut]): Unit =
        psLogic.close(wrapPS(ps))

      override def open(parameters: Configuration, runtimeContext: RuntimeContext): Unit =
        psLogic.open(parameters, runtimeContext)
    }

    val wrappedWorkerInPartition: PSToWorker[IntOrId, EOFOrP] => Int = {
      case PSToWorker(workerPartitionIndex, PullAnswer(paramId, Right(param))) =>
        wInPartition(PSToWorker(workerPartitionIndex, PullAnswer(paramId.right.get, param)))
    }

    transform[ModelOrT, IntOrId, EOFOrP, PSOut, WOut, PSToWorker[IntOrId, EOFOrP], WorkerToPS[IntOrId, EOFOrP]](
      modelWithEOF.union(trainingDataPrepared.setParallelism(workerParallelism)),
      wrappedWorkerLogic,
      wrappedPSLogic,
      wrappedParamPartitioner,
      wrappedWorkerInPartition,
      workerParallelism,
      psParallelism,
      new SimpleWorkerReceiver[IntOrId, EOFOrP],
      new SimpleWorkerSender[IntOrId, EOFOrP],
      new SimplePSReceiver[IntOrId, EOFOrP],
      new SimplePSSender[IntOrId, EOFOrP],
      iterationWaitTime
    )
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
    * @tparam Id
    * Type of parameter identifier.
    * @tparam P
    * Type of parameter.
    * @tparam PSOut
    * Type of output of PS.
    * @tparam WOut
    * Type of output of workers.
    * @return
    * Transform [[DataStream]] consisting of the worker and PS output.
    */
  def transformWithDoubleModelLoad[T, Id, P, PSOut, WOut] (model: DataStream[Either[(Id, P), (Id, P)]])
                                                             (trainingData: DataStream[T],
                                                              workerLogic: BaseMFWorkerLogic[T, Id, P, WOut],
                                                              psLogic: ParameterServerLogic[Id, P, PSOut],
                                                              paramPartitioner: WorkerToPS[Id, P] => Int,
                                                              wInPartition: PSToWorker[Id, P] => Int,
                                                              workerParallelism: Int,
                                                              psParallelism: Int,
                                                              iterationWaitTime: Long)
                                                             (implicit
                                                              tiT: TypeInformation[T],
                                                              tiId: TypeInformation[Id],
                                                              tiP: TypeInformation[P],
                                                              tiPSOut: TypeInformation[PSOut],
                                                              tiWOut: TypeInformation[WOut]
                                                             ): DataStream[Either[WOut, PSOut]] = {

    sealed abstract class IdxOrId extends Serializable {
      def serverIndex : Int
      def identifier : Id
    }
    case class ServerIndex(idx : Int) extends IdxOrId {
      override def serverIndex: Int = idx
      override def identifier: Id = throw new IllegalStateException("ServerIndex instead of Identifier")
    }
    case class Identifier(id : Id) extends IdxOrId {
      override def serverIndex: Int = throw new IllegalStateException("Identifier instead of ServerIndex")
      override def identifier: Id = id
    }

    sealed abstract class ModelOrT extends Serializable
    case class ModelWorkerData(id: Id, param: P) extends ModelOrT
    sealed abstract class ServerInput extends ModelOrT

    case class Parameter(id: Id, param: P) extends ServerInput
    case class TrainingData(data: T) extends ModelOrT
    case class EOF() extends ServerInput

    val modelWithEOF: DataStream[ModelOrT] =
      model.rebalance.map(x => x).setParallelism(workerParallelism)
        .forward.flatMap(new RichFlatMapFunction[Either[(Id, P), (Id, P)], ModelOrT] {

        var collector: Collector[ModelOrT] = _

        override def flatMap(value: Either[(Id, P), (Id, P)], out: Collector[ModelOrT]): Unit = {
          if (collector == null) {
            collector = out
          }
          out.collect(value match {
            case Left((id, data)) => Parameter(id, data)
            case Right((id, data)) => ModelWorkerData(id, data)
          })
        }

        override def close(): Unit = {
          if (collector != null) {
            collector.collect(EOF())
          } else {
            throw new IllegalStateException("There must be a parameter per model partition when loading model.")
          }
        }
      }).setParallelism(workerParallelism)

    val trainingDataPrepared: DataStream[ModelOrT] = trainingData.rebalance.map(x => x).setParallelism(workerParallelism)
      .forward.map(TrainingData)

    // TODO do not wrap PSClient every time it's used
    def wrapPSClient(ps: ParameterServerClient[IdxOrId, ServerInput, WOut]): ParameterServerClient[Id, P, WOut] =
      new ParameterServerClient[Id, P, WOut] {
        override def pull(id: Id): Unit = ps.pull(Identifier(id))

        override def push(id: Id, deltaUpdate: P): Unit = ps.push(Identifier(id), Parameter(id, deltaUpdate))

        override def output(out: WOut): Unit = ps.output(out)
      }

    val wrappedWorkerLogic = new BaseMFWorkerLogic[ModelOrT, IdxOrId, ServerInput, WOut] {

      var receivedEOF = false
      val dataBuffer = new ArrayBuffer[T]()

      override def onRecv(modelOrDataPoint: ModelOrT, ps: ParameterServerClient[IdxOrId, ServerInput, WOut]): Unit = {
        modelOrDataPoint match {
          case EOF() =>
            receivedEOF = true

            // notify all PS instance
            (0 until psParallelism).foreach {
              psIdx => ps.push(ServerIndex(psIdx), EOF())
            }

            // process buffered data
            val wrappedPS = wrapPSClient(ps)
            dataBuffer.foreach {
              dataPoint => workerLogic.onRecv(dataPoint, wrappedPS)
            }
          case Parameter(id, paramValue) =>
            ps.push(Identifier(id), Parameter(id, paramValue))


          case ModelWorkerData(itemId, paramValue) =>
            workerLogic.updateModel(itemId, paramValue)
          case TrainingData(dataPoint) =>
            if (receivedEOF) {
              workerLogic.onRecv(dataPoint, wrapPSClient(ps))
            } else {
              dataBuffer.append(dataPoint)
            }
        }
      }

      override def onPullRecv(paramId: IdxOrId,
                              paramValue: ServerInput,
                              ps: ParameterServerClient[IdxOrId, ServerInput, WOut]): Unit = {
        paramValue match {
          case Parameter(_, p) =>
            workerLogic.onPullRecv(paramId.identifier, p, wrapPSClient(ps))
          case _ =>
          // do nothing with EOF responses
        }
      }

      override def close(): Unit = {
        workerLogic.close()
      }

      override def updateModel(id: IdxOrId, param: ServerInput): Unit = {
        param match {
          case Parameter(_id, p) =>
            workerLogic.updateModel(_id, p)
          case _ =>
          // do nothing with EOF responses
        }
      }
    }

    val wrappedParamPartitioner: WorkerToPS[IdxOrId, ServerInput] => Int = {
      case WorkerToPS(workerPartitionIndex, msg) => msg match {
        case Left(Pull(paramId)) =>
          paramPartitioner(WorkerToPS(workerPartitionIndex, Left(Pull(paramId.identifier))))
        case Right(Push(paramId, deltaOrEOF)) => deltaOrEOF match {
          case EOF() => paramId.serverIndex
          case Parameter(_, delta) => paramPartitioner(WorkerToPS(workerPartitionIndex,Right(Push(paramId.identifier, delta))))
        }
      }
    }

    // TODO do not wrap PS every time it's used
    def wrapPS(ps: ParameterServer[IdxOrId, ServerInput, PSOut]): ParameterServer[Id, P, PSOut] =
      new ParameterServer[Id, P, PSOut] {

        override def answerPull(id: Id, value: P, workerPartitionIndex: Int): Unit =
          ps.answerPull(Identifier(id), Parameter(id, value), workerPartitionIndex)

        override def output(out: PSOut): Unit =
          ps.output(out)
      }

    val wrappedPSLogic = new ParameterServerLogic[IdxOrId, ServerInput, PSOut] {

      var eofCountDown: Int = workerParallelism

      val pullBuffer = new ArrayBuffer[(Id, Int)]()

      override def onPullRecv(id: IdxOrId, workerPartitionIndex: Int, ps: ParameterServer[IdxOrId, ServerInput, PSOut]): Unit = {
        if (eofCountDown == 0) {
          psLogic.onPullRecv(id.identifier, workerPartitionIndex, wrapPS(ps))
        } else {
          pullBuffer.append((id.identifier, workerPartitionIndex))
        }
      }

      override def onPushRecv(id: IdxOrId,
                              deltaUpdate: ServerInput,
                              ps: ParameterServer[IdxOrId, ServerInput, PSOut]): Unit = {
        deltaUpdate match {
          case EOF() =>
            eofCountDown -= 1

            if (eofCountDown == 0) {
              // we have received the model, we can process the buffered pulls
              pullBuffer.foreach {
                case (paramId, workerPartitionIndex) =>
                  psLogic.onPullRecv(paramId, workerPartitionIndex, wrapPS(ps))
              }
            }
          case Parameter(_, param) =>
            if (eofCountDown > 0) {
              // send an EOF so that iteration wait time is not exceeded during model load
              ps.answerPull(id, EOF(), ((id.hashCode % workerParallelism) + workerParallelism) % workerParallelism)
            }
            psLogic.onPushRecv(id.identifier, param, wrapPS(ps))
        }

      }

      override def close(ps: ParameterServer[IdxOrId, ServerInput, PSOut]): Unit =
        psLogic.close(wrapPS(ps))

      override def open(parameters: Configuration, runtimeContext: RuntimeContext): Unit =
        psLogic.open(parameters, runtimeContext)
    }

    val wrappedWorkerInPartition: PSToWorker[IdxOrId, ServerInput] => Int = {
      case PSToWorker(workerPartitionIndex, PullAnswer(paramId, Parameter(_, param))) =>
        wInPartition(PSToWorker(workerPartitionIndex, PullAnswer(paramId.identifier, param)))
      case PSToWorker(workerPartitionIndex, PullAnswer(_, EOF())) => workerPartitionIndex
    }

    transform[ModelOrT, IdxOrId, ServerInput, PSOut, WOut, PSToWorker[IdxOrId, ServerInput], WorkerToPS[IdxOrId, ServerInput]](
      modelWithEOF.union(trainingDataPrepared.setParallelism(workerParallelism)),
      wrappedWorkerLogic,
      wrappedPSLogic,
      wrappedParamPartitioner,
      wrappedWorkerInPartition,
      workerParallelism,
      psParallelism,
      new SimpleWorkerReceiver[IdxOrId, ServerInput],
      new SimpleWorkerSender[IdxOrId, ServerInput],
      new SimplePSReceiver[IdxOrId, ServerInput],
      new SimplePSSender[IdxOrId, ServerInput],
      iterationWaitTime
    )
  }

  /**
    * Connects [[ParameterServer]] and [[PSSender]] to Flink logic.
    */
  private class MessagingPS[WorkerIn, WorkerOut, Id, P, PSOut](psSender: PSSender[WorkerIn, Id, P])
    extends ParameterServer[Id, P, PSOut] {

    private var collector: Collector[Either[WorkerIn, PSOut]] = _

    def setCollector(out: Collector[Either[WorkerIn, PSOut]]): Unit = {
      collector = out
    }

    def collectAnswerMsg(msg: WorkerIn): Unit = {
      collector.collect(Left(msg))
    }

    override def answerPull(id: Id, value: P, workerPartitionIndex: Int): Unit = {
      psSender.onPullAnswer(id, value, workerPartitionIndex, collectAnswerMsg)
    }

    override def output(out: PSOut): Unit = {
      collector.collect(Right(out))
    }
  }

  /**
    * Connects [[ParameterServerClient]] and [[WorkerSender]] to Flink logic.
    */
  private class MessagingPSClient[IN, OUT, Id, P, WOut](sender: WorkerSender[OUT, Id, P])
    extends ParameterServerClient[Id, P, WOut] {

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

    override def pull(id: Id): Unit =
      sender.onPull(id, collectPullMsg, partitionId)

    override def push(id: Id, deltaUpdate: P): Unit =
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
  * @tparam Id
  * Type of parameter identifiers.
  * @tparam P
  * Type of parameters.
  * @tparam PSOut
  * Type of ParameterServer output.
  */
trait ParameterServerLogic[Id, P, PSOut] extends LooseParameterServerLogic[Id, P, P, PSOut]

/**
  * Logic of the ParameterServer, that stores the parameters,
  * applies pushes and answers pulls.
  *
  * This could be implemented if needed (e.g. for controlling the output of parameters),
  * but it is not necessary to have a custom implementation.
  * By default a [[hu.sztaki.ilab.ps.server.SimplePSLogic]] is used.
  *
  * @tparam Id
  * Type of parameter identifiers.
  * @tparam PullP
  * Type of Pull parameters.
  * @tparam PushP
  * Type of Push parameters.
  * @tparam PSOut
  * Type of ParameterServer output.
  */
trait LooseParameterServerLogic[Id, PullP, PushP, PSOut] extends Serializable {

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
  def onPullRecv(id: Id, workerPartitionIndex: Int, ps: ParameterServer[Id, PullP, PSOut]): Unit

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
  def onPushRecv(id: Id, deltaUpdate: PushP, ps: ParameterServer[Id, PullP, PSOut]): Unit

  /**
    * Method called when processing is finished.
    */
  def close(ps: ParameterServer[Id, PullP, PSOut]): Unit = ()

  /**
    * Method called when the class is initialized.
    */
  def open(parameters: Configuration, runtimeContext: RuntimeContext): Unit = ()
}


trait ParameterServer[Id, P, PSOut] extends Serializable {
  def answerPull(id: Id, value: P, workerPartitionIndex: Int): Unit

  def output(out: PSOut): Unit
}

/**
  * Logic of forming the messages received at PS from a worker to a pulls and pushes.
  *
  * @tparam WorkerToPS
  * Type of message from workers to PS.
  * @tparam Id
  * Type of parameter identifier.
  * @tparam P
  * Type of parameter.
  */
trait PSReceiver[WorkerToPS, Id, P] extends Serializable {
  def onWorkerMsg(msg: WorkerToPS,
                  onPullRecv: (Id, Int) => Unit,
                  onPushRecv: (Id, P) => Unit)
}

/**
  * Logic of wrapping the pull answers into messages sent by PS to worker.
  *
  * @tparam PStoWorker
  * Type of message from PS to workers.
  * @tparam Id
  * Type of parameter identifier.
  * @tparam P
  * Type of parameter.
  */
trait PSSender[PStoWorker, Id, P] extends Serializable {
  def onPullAnswer(id: Id,
                   value: P,
                   workerPartitionIndex: Int,
                   collectAnswerMsg: PStoWorker => Unit)
}

/**
  * Logic of forming the messages received at worker from PS to a pull answer.
  *
  * @tparam PStoWorker
  * Type of message from PS to workers.
  * @tparam Id
  * Type of parameter identifier.
  * @tparam P
  * Type of parameter.
  */
trait WorkerReceiver[PStoWorker, Id, P] extends Serializable {
  def onPullAnswerRecv(msg: PStoWorker, pullHandler: PullAnswer[Id, P] => Unit)
}

/**
  * Logic of wrapping the pulls and pushes into messages sent by worker to PS.
  *
  * @tparam WorkerToPS
  * Type of message from workers to PS.
  * @tparam Id
  * Type of parameter identifier.
  * @tparam P
  * Type of parameter.
  */
trait WorkerSender[WorkerToPS, Id, P] extends Serializable {
  def onPull(id: Id, collectAnswerMsg: WorkerToPS => Unit, partitionId: Int)

  def onPush(id: Id, deltaUpdate: P, collectAnswerMsg: WorkerToPS => Unit, partitionId: Int)
}

