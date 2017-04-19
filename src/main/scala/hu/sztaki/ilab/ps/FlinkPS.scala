package hu.sztaki.ilab.ps

import hu.sztaki.ilab.ps.entities.PullAnswer
import org.apache.flink.api.common.functions.{Partitioner, RichFlatMapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
  * Client interface for the ParameterServer that the worker can use.
  * This can be used in [[WorkerLogic]].
  *
  * @tparam P
  * Type of the parametes.
  * @tparam WorkerOut
  * Type of the worker output.
  */
trait ParameterServerClient[P, WorkerOut] extends Serializable {

  def pull(id: Int): Unit

  def push(id: Int, deltaUpdate: P): Unit

  def output(out: WorkerOut): Unit

}

trait ClientReceiver[IN, P] extends Serializable {
  def onPullAnswerRecv(msg: IN, pullHandler: PullAnswer[P] => Unit)
}

trait ClientSender[OUT, P] extends Serializable {
  def onPull(id: Int, collectAnswerMsg: OUT => Unit, partitionId: Int)

  def onPush(id: Int, deltaUpdate: P, collectAnswerMsg: OUT => Unit, partitionId: Int)
}

trait WorkerLogic[T, P, WOut] extends Serializable {

  def onRecv(data: T, ps: ParameterServerClient[P, WOut]): Unit

  def onPullRecv(paramId: Int, paramValue: P, ps: ParameterServerClient[P, WOut]): Unit

  def close(): Unit = ()
}

/* PS message flow */

trait PSReceiver[WorkerToPS, P] extends Serializable {
  def onWorkerMsg(msg: WorkerToPS,
                  onPullRecv: (Int, Int) => Unit,
                  onPushRecv: (Int, P) => Unit)
}

trait ParameterServerLogic[P, PSOut] extends Serializable {

  def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[P, PSOut]): Unit

  def onPushRecv(id: Int, deltaUpdate: P, ps: ParameterServer[P, PSOut]): Unit
}

trait ParameterServer[P, PSOut] extends Serializable {
  def answerPull(id: Int, value: P, workerPartitionIndex: Int): Unit

  def output(out: PSOut): Unit
}

trait PSSender[PStoWorker, P] extends Serializable {
  def onPullAnswer(id: Int,
                   value: P,
                   workerPartitionIndex: Int,
                   collectAnswerMsg: PStoWorker => Unit)
}

class FlinkPS

object FlinkPS {

  private val log = LoggerFactory.getLogger(classOf[FlinkPS])

  def psTransform[T, P, PSOut, WOut, PStoWorker, WorkerToPS](xs: DataStream[T],
                                                             workerLogic: WorkerLogic[T, P, WOut],
                                                             psLogic: ParameterServerLogic[P, PSOut],
                                                             clientReceiver: ClientReceiver[PStoWorker, P],
                                                             clientSender: ClientSender[WorkerToPS, P],
                                                             psReceiver: PSReceiver[WorkerToPS, P],
                                                             psSender: PSSender[PStoWorker, P],
                                                             paramPartitioner: WorkerToPS => Int,
                                                             wInPartition: PStoWorker => Int,
                                                             workerParallelism: Int,
                                                             psParallelism: Int,
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

            val receiver: ClientReceiver[PStoWorker, P] = clientReceiver
            val sender: ClientSender[WorkerToPS, P] = clientSender
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

    xs
      .map(x => x)
      .setParallelism(workerParallelism)
      .iterate((x: ConnectedStreams[T, PStoWorker]) => stepFunc(x), iterationWaitTime)
  }

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

  private class MessagingPSClient[IN, OUT, P, WOut](sender: ClientSender[OUT, P])
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

