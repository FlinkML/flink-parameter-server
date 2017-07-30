package hu.sztaki.ilab.ps

import hu.sztaki.ilab.ps.entities.PullAnswer
import org.apache.flink.streaming.api.scala._
import org.scalatest._
import prop._

import scala.collection.mutable

class FlinkParameterServerTest extends FlatSpec with PropertyChecks with Matchers {

  import hu.sztaki.ilab.ps.FlinkParameterServer._

  "flink mock PS" should "work" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setBufferTimeout(10)
    val iterWaitTime = 4000

    val src = env.fromCollection(Seq(0, 1, 2, 3, 4, 5, 6, 7, 9))
      .map(x => x).setParallelism(4)

    // Two params: for even values and for odd values.
    // Parameters are queues.
    // Data is just appended to the appropriate queue.

    type P = mutable.Queue[Int]
    type WorkerIn = (Int, Array[String])
    type WorkerOut = (Boolean, Array[Int])
    type WOut = Unit

    val outputDS =
      transform(src,
        new WorkerLogic[Int, P, WOut] {
          val dataQ = new mutable.Queue[Int]()

          override def onRecv(data: Int, ps: ParameterServerClient[P, WOut]): Unit = {
            dataQ.enqueue(data)
            ps.pull(data % 2)
          }

          override def onPullRecv(paramId: Int, paramValue: P, ps: ParameterServerClient[P, WOut]): Unit = {
            val xs = dataQ.dequeueAll(x => x % 2 == paramId)
            ps.push(paramId, mutable.Queue(xs: _*))
          }
        },
        new ParameterServerLogic[P, String] {
          val params = new mutable.HashMap[Int, mutable.Queue[Int]]()

          override def onPullRecv(id: Int, workerPartitionIndex: Int, ps: ParameterServer[P, String]): Unit = {
            val param = params.getOrElseUpdate(id, new mutable.Queue[Int]())
            ps.answerPull(id, param, workerPartitionIndex)
          }

          override def onPushRecv(id: Int, deltaUpdate: P, ps: ParameterServer[P, String]): Unit = {
            params(id).enqueue(deltaUpdate: _*)
            ps.output(params(id).mkString(","))
          }

        },
        (x: (Boolean, Array[Int])) => x match {
          case (true, Array(partitionId, id)) => Math.abs(id.hashCode())
          case (false, Array(id, _*)) => Math.abs(id.hashCode())
        },
        (x: (Int, Array[String])) => x._2.head.toInt,
        4,
        4,
        new WorkerReceiver[WorkerIn, P] {
          override def onPullAnswerRecv(msg: WorkerIn, pullHandler: PullAnswer[P] => Unit): Unit = {
            pullHandler(PullAnswer(msg._1, mutable.Queue(msg._2.tail.map(_.toInt): _*)))
          }
        },
        new WorkerSender[WorkerOut, P] {
          override def onPull(id: Int, collectAnswerMsg: WorkerOut => Unit, partitionId: Int): Unit = {
            collectAnswerMsg((true, Array(partitionId, id)))
          }

          override def onPush(id: Int, deltaUpdate: P, collectAnswerMsg: WorkerOut => Unit, partitionId: Int): Unit = {
            collectAnswerMsg((false, Array(id, deltaUpdate: _*)))
          }
        },
        new PSReceiver[(Boolean, Array[Int]), P] {
          override def onWorkerMsg(msg: (Boolean, Array[Int]), onPullRecv: (Int, Int) => Unit, onPushRecv: (Int, P) => Unit): Unit = {
            msg match {
              case (true, Array(partitionId, id)) =>
                onPullRecv(id, partitionId)
              case (false, arr) =>
                onPushRecv(arr.head, mutable.Queue(arr.tail: _*))
            }
          }
        },
        new PSSender[WorkerIn, P] {
          override def onPullAnswer(id: Int,
                                    value: P,
                                    workerPartitionIndex: Int,
                                    collectAnswerMsg: ((Int, Array[String])) => Unit): Unit = {
            collectAnswerMsg((id, (workerPartitionIndex +: value).map(_.toString).toArray))
          }
        },
        5000
      )

    outputDS.print()

    env.execute()

  }

}
