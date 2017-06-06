package hu.sztaki.ilab.ps

import hu.sztaki.ilab.ps.FlinkParameterServer._
import hu.sztaki.ilab.ps.client.receiver.MultipleWorkerReceiver
import hu.sztaki.ilab.ps.client.sender._
import hu.sztaki.ilab.ps.common.Combinable
import hu.sztaki.ilab.ps.entities.{PSToWorker, WorkerToPS}
import hu.sztaki.ilab.ps.server.SimplePSLogic
import hu.sztaki.ilab.ps.server.receiver.MultiplePSReceiver
import hu.sztaki.ilab.ps.server.sender.{CombinationPSSender, CountPSSender, TimerPSSender}
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable
import scala.concurrent.duration._

class FlinkCombinationStackTest extends FlatSpec with PropertyChecks with Matchers {

  "flink combined count and timer worker and PS communication" should "work" in {

    // @todo ClientSender is of an array type, since we buffer the data before sending!
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

    val countLimit = 4
    val timeLimit = 1 seconds

    val clientCombinables: List[Combinable[WorkerToPS[P]]] =
      List(CountClientSender(countLimit), TimerClientSender(timeLimit))

    val serverCombinables: List[Combinable[PSToWorker[P]]] =
      List(CountPSSender(countLimit), TimerPSSender(timeLimit))

    // The counter AND the timer condition should be met at the same time before the client sends
    def clientCondition(combinables: List[Combinable[WorkerToPS[P]]]): Boolean = {
      combinables.map(_.shouldSend).reduce(_ && _)
    }

    // The counter OR the timer condition should be met before the server sends
    def serverCondition(combinables: List[Combinable[PSToWorker[P]]]): Boolean = {
      combinables.map(_.shouldSend).reduce(_ || _)
    }

    val combinoClientSender = new CombinationWorkerSender[P](clientCondition, clientCombinables)
    val combinoPSSender = new CombinationPSSender[P](serverCondition, serverCombinables)

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
      FlinkParameterServer.transform(
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
        // @todo proper partitioning, this is just a placeholder
        (data: Array[WorkerToPS[P]]) => {
          data.head.workerPartitionIndex
        },
        (data: Array[PSToWorker[P]]) => {
          data.length match {
            case 0 => 0
            case _ => data.head.workerPartitionIndex % numberOfPartitions
          }
        },
        4,
        4,
        new MultipleWorkerReceiver[P],
        combinoClientSender,
        new MultiplePSReceiver[P],
        combinoPSSender,
        5000
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

}