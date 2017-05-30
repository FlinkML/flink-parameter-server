package hu.sztaki.ilab.ps

import org.apache.flink.streaming.api.scala._
import org.scalatest._
import prop._
import hu.sztaki.ilab.ps.FlinkParameterServer._
import hu.sztaki.ilab.ps.client.receiver.SimpleWorkerReceiver
import hu.sztaki.ilab.ps.client.sender.SimpleWorkerSender
import hu.sztaki.ilab.ps.entities.{PSToWorker, Pull, Push, WorkerToPS}
import hu.sztaki.ilab.ps.server.SimplePSLogic
import hu.sztaki.ilab.ps.server.receiver.SimplePSReceiver
import hu.sztaki.ilab.ps.server.sender.SimplePSSender
import hu.sztaki.ilab.ps.test.utils.FlinkTestUtils
import hu.sztaki.ilab.ps.test.utils.FlinkTestUtils.SuccessException
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class FlinkSimpleStackTest extends FlatSpec with PropertyChecks with Matchers {

  private val log = LoggerFactory.getLogger(classOf[FlinkSimpleStackTest])

  "flink simple worker and PS communication" should "work" in {

    type P = Array[Double]
    type T = (Int, P)
    type WOut = Unit

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setBufferTimeout(10)
    val iterWaitTime = 4000

    val numberOfPartitions = 4
    val arrayLength = 5

    class ByRowPartitioner extends Partitioner[T] {
      override def partition(key: T, numPartitions: Int): Int = {
        key._1 % numPartitions
      }
    }

    val src = env.fromCollection(Seq[(Int, Array[Double])](
      (1, Array[Double](1.5, 5.3, 1.3, 5.6, 7.9)),
      (2, Array[Double](0.1, 0.1, 0.1, 0.1, 0.1)),
      (2, Array[Double](10.1, 10.2, 10.3, 10.4, 10.5)),
      (3, Array[Double](20.5, 26.3, 28.1, 29.2, 29.7)),
      (5, Array[Double](100, 101, 102, 103, 104)),
      (4, Array[Double](4.5, 9.6, 2.3, 9.9, 0.5)),
      (5, Array[Double](4.8, 15, 16, 23, 42)),
      (1, Array[Double](1000, 1000, 1000, 1000, 1000))
    ))
      .map(x => x).partitionCustom(new ByRowPartitioner(), data => data).setParallelism(numberOfPartitions)

    def initPS(id: Int): P = {
      Array.fill(arrayLength)(0.0)
    }

    def updatePS(original: P, delta: P): P = {
      if (original.length != delta.length) {
        throw new Exception("The vector sizes should be equal. Shame!")
      }
      (original, delta).zipped.map(_ + _)
    }

    val outputDS =
      parameterServerTransform(
        // @todo add real source
        src,
        // @todo add real worker logic
        new WorkerLogic[T, P, WOut] {
          val waitingToAnswer = new mutable.HashMap[Int, mutable.Queue[Array[Double]]]()

          override def onRecv(data: T, ps: ParameterServerClient[P, WOut]): Unit = {
            ps.pull(data._1)
            val waitingQueue = waitingToAnswer.getOrElseUpdate(data._1, new mutable.Queue[Array[Double]]())
            waitingQueue += data._2
          }

          override def onPullRecv(paramId: Int, paramValue: P, ps: ParameterServerClient[P, WOut]): Unit = {
            println(s"Received stuff: ${paramId}, ${paramValue.mkString(",")}")
            val delta = waitingToAnswer.get(paramId) match {
              case Some(q) => q.dequeue()
              case None => throw new IllegalStateException("Something went wrong.")
            }
            // make some calculation with paramValue + delta. We skip it.
            ps.push(paramId, delta)
          }
        },
        new SimplePSLogic[P](initPS, updatePS),
        (x: WorkerToPS[P]) => x.workerPartitionIndex,
        (x: PSToWorker[P]) => x.workerPartitionIndex % numberOfPartitions,
        4,
        4,
        new SimpleWorkerReceiver[P],
        new SimpleWorkerSender[P],
        new SimplePSReceiver[P],
        new SimplePSSender[P]
      )

    outputDS.map(
      // logger fails here
      x => x match {
        case Right(record) => println(s"ID: ${record._1}, DATA: ${record._2.mkString(",")}")
        case _ => ()
      }
    )

    outputDS.print()

    env.execute()

  }


  "model load" should "work" in {

    type P = Long
    type T = Int
    type PSOut = (Int, P)
    type WOut = Unit

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setBufferTimeout(10)
    val iterWaitTime = 4000

    val numParams = 50

    val workerParallelism = 4
    val psParallelism = 3
    val modelSrcParallelism = 2
    val dataSrcParallelism = 3

    val initModel: Seq[(Int, P)] = Seq.tabulate(numParams)(i => (i, i * 10))
    val expectedOutModel: Seq[(Int, P)] = Seq.tabulate(numParams)(i => (i, i * 10 + 3))

    val trainingData: Seq[Int] = Random.shuffle(
      (0 until numParams).flatMap(i => Iterable(i, i, i))
    )

    val modelSrc = env.fromCollection(initModel).shuffle.map(x => x).setParallelism(modelSrcParallelism)
    val dataSrc = env.fromCollection(trainingData).shuffle.map(x => x).setParallelism(dataSrcParallelism)

    val outputDS: DataStream[Either[WOut, PSOut]] =
      FlinkParameterServer.transformWithModelLoad(modelSrc)(
        dataSrc,
        new WorkerLogic[T, P, WOut] {
          override def onRecv(data: Int, ps: ParameterServerClient[P, WOut]): Unit = {
            ps.pull(data)
          }

          override def onPullRecv(paramId: Int, paramValue: P, ps: ParameterServerClient[P, WOut]): Unit = {
            ps.push(paramId, 1L)
          }
        },
        new ParameterServerLogic[P, PSOut] {
          val params = new mutable.HashMap[Int, P]()

          override def onPullRecv(id: T, workerPartitionIndex: T, ps: ParameterServer[P, (T, P)]): WOut = {
            ps.answerPull(id, params.getOrElseUpdate(id, 0), workerPartitionIndex)
          }

          override def onPushRecv(id: T, deltaUpdate: P, ps: ParameterServer[P, (T, P)]): WOut = {
            params.put(id, params.getOrElseUpdate(id, 0) + deltaUpdate)
          }

          override def close(ps: ParameterServer[P, (T, P)]): WOut = {
            params.foreach(ps.output)
          }
        }, {
          case WorkerToPS(_, msg) => msg match {
            case Left(Pull(id)) => id % psParallelism
            case Right(Push(id, _)) => id % psParallelism
          }
        }, {
          case PSToWorker(wIdx, _) => wIdx
        },
        workerParallelism, psParallelism, iterWaitTime
      )

    outputDS.addSink(new RichSinkFunction[Either[WOut, (Int, P)]] {
      val result = new ArrayBuffer[(Int, P)]
      override def invoke(value: Either[WOut, (Int, P)]): Unit = {
        value match {
          case Right(param) => result.append(param)
          case _ => throw new IllegalStateException()
        }
      }

      override def close(): Unit = {
        throw new SuccessException[Seq[(Int, P)]](result)
      }
    }).setParallelism(1)

    FlinkTestUtils.executeWithSuccessCheck[Seq[(Int, P)]](env) {
      result =>
        result.sorted should be (expectedOutModel)
    }

  }
}
