package hu.sztaki.ilab.ps

import hu.sztaki.ilab.ps.entities.{PSToWorker, Pull, Push, WorkerToPS}
import hu.sztaki.ilab.ps.test.utils.FlinkTestUtils
import hu.sztaki.ilab.ps.test.utils.FlinkTestUtils.SuccessException
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.scalatest._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class FlinkStringIdentifierTest extends FlatSpec with Matchers {

  type T = String
  type Id = String
  type P = Int
  type PullP = P
  type PushP = Boolean
  type PSOut = (Id, P)
  type WOut = (Id, P)

  val text: Array[String] =
    """a a b a c d a a b a a abd lje jdf aa
      |a a a de ae a c a b a a a a a b
      |aa bb B a b a b A a a b de ade ana de
      |egf ade pi me a dj fe nb df ds ae ae ae""".stripMargin.split(" ")

  val expected: Map[String, Int] = {
    val map = new mutable.HashMap[String, Int]
    text foreach (w => map(w) = map.getOrElse(w, 0) + 1)
    map("???") = 0 // special output
    map.toMap
  }

  "flink word counter" should "work with simple worker and PS" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.fromCollection(text)

    val worker = new WorkerLogic[T, Id, P, WOut] {

      override def onRecv(data: T, ps: ParameterServerClient[Id, P, WOut]): Unit = {
        ps.pull(data)
      }

      override def onPullRecv(paramId: Id, paramValue: P, ps: ParameterServerClient[Id, P, WOut]): Unit = {
        ps.push(paramId, 1)
        ps.output(("???", 0))
      }
    }

    FlinkParameterServer
      .transform[T, Id, P, WOut](stream, worker, (_: Id) => 0, (p1: P, p2: P) => p1 + p2, 4, 4, 5000L)
      .map(_ match {
        case Left(left) => left
        case Right(right) => right
      })
      .addSink(new RichSinkFunction[WOut] {
        val result = new mutable.HashMap[String, Int]

        override def invoke(value: WOut): Unit = {
          result.put(value._1, Math.max(result.getOrElse(value._1, 0), value._2))
        }

        override def close(): Unit = {
          throw SuccessException(result.toMap)
        }
      }).setParallelism(1)

    FlinkTestUtils.executeWithSuccessCheck[Map[String, Int]](env) {
      result => result should be(expected)
    }


  }

  it should "work with loose worker" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.fromCollection(text)

    val worker = new LooseWorkerLogic[T, Id, PullP, PushP, WOut] {

      override def onRecv(data: T, ps: ParameterServerClient[Id, PushP, WOut]): Unit = {
        ps.pull(data)
      }

      override def onPullRecv(paramId: Id, paramValue: PullP, ps: ParameterServerClient[Id, PushP, WOut]): Unit = {
        ps.push(paramId, true)
        ps.output(("???", 0))
      }
    }

    FlinkParameterServer
      .transform[T, Id, PullP, PushP, WOut](stream, worker, (_: Id) => 0, (p: PullP, b: PushP) => if (b) p + 1 else p, 4, 4, 5000L)
      .map(_ match {
        case Left(left) => left
        case Right(right) => right
      })
      .addSink(new RichSinkFunction[WOut] {
        val result = new mutable.HashMap[String, Int]

        override def invoke(value: WOut): Unit = {
          result.put(value._1, Math.max(result.getOrElse(value._1, 0), value._2))
        }

        override def close(): Unit = {
          throw SuccessException(result.toMap)
        }
      }).setParallelism(1)

    FlinkTestUtils.executeWithSuccessCheck[Map[String, Int]](env) {
      result => result should be(expected)
    }

  }

  it should "work with explicit PS logic" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.fromCollection(text)

    val worker = new LooseWorkerLogic[T, Id, PullP, PushP, WOut] {

      override def onRecv(data: T, ps: ParameterServerClient[Id, PushP, WOut]): Unit = {
        ps.pull(data)
      }

      override def onPullRecv(paramId: Id, paramValue: PullP, ps: ParameterServerClient[Id, PushP, WOut]): Unit = {
        ps.push(paramId, true)
      }
    }

    val psLogic = new LooseParameterServerLogic[Id, PullP, PushP, PSOut] {
      val map: mutable.HashMap[String, Int] = new mutable.HashMap

      override def onPullRecv(id: Id, workerPartitionIndex: PullP, ps: ParameterServer[Id, PullP, (Id, P)]): Unit = {
        val newValue = map.getOrElseUpdate(id, 0) + 1
        map(id) = newValue
        ps.answerPull(id, newValue, workerPartitionIndex)
      }

      override def onPushRecv(id: Id, deltaUpdate: PushP, ps: ParameterServer[Id, PullP, (Id, P)]): Unit = {
        if (deltaUpdate) {
          ps.output(id, map(id))
        }
      }

      override def close(ps: ParameterServer[Id, PullP, (Id, P)]): Unit = {
        super.close(ps)
        ps.output(("???", 0))
      }

    }

    FlinkParameterServer
      .transform[T, Id, PullP, PushP, PSOut, WOut](stream, worker, psLogic, 4, 4, 5000L)
      .map(_ match {
        case Left(left) => left
        case Right(right) => right
      })
      .addSink(new RichSinkFunction[WOut] {
        val result = new mutable.HashMap[String, Int]

        override def invoke(value: WOut): Unit = {
          result(value._1) = Math.max(result.getOrElse(value._1, 0), value._2)
        }

        override def close(): Unit = {
          throw SuccessException(result.toMap)
        }
      }).setParallelism(1)

    FlinkTestUtils.executeWithSuccessCheck[Map[String, Int]](env) {
      result => result should be(expected)
    }

  }

  "model load with string params" should "work" in {

    type T = String
    type Id = String
    type P = Long
    type PSOut = (Id, P)
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

    val initModel: Seq[(Id, P)] = Seq.tabulate(numParams)(i => ("id" + i, i * 10L))
    val expectedOutModel: Seq[(Id, P)] = Seq.tabulate(numParams)(i => ("id" + i, i * 10L + 3)).sorted

    val trainingData: Seq[String] = Random.shuffle(
      (0 until numParams).flatMap(i => Iterable("id" + i, "id" + i, "id" + i))
    )

    val modelSrc = env.fromCollection(initModel).shuffle.map(x => x).setParallelism(modelSrcParallelism)
    val dataSrc = env.fromCollection(trainingData).shuffle.map(x => x).setParallelism(dataSrcParallelism)

    val outputDS: DataStream[Either[WOut, PSOut]] =
      FlinkParameterServer.transformWithModelLoad(modelSrc)(
        dataSrc,
        new WorkerLogic[T, Id, P, WOut] {
          override def onRecv(data: T, ps: ParameterServerClient[Id, P, WOut]): Unit = {
            ps.pull(data)
          }

          override def onPullRecv(paramId: Id, paramValue: P, ps: ParameterServerClient[Id, P, WOut]): Unit = {
            ps.push(paramId, 1L)
          }
        },
        new ParameterServerLogic[T, P, PSOut] {
          val params = new mutable.HashMap[Id, P]()

          override def onPullRecv(id: Id, workerPartitionIndex: Int, ps: ParameterServer[Id, P, PSOut]): WOut = {
            ps.answerPull(id, params.getOrElseUpdate(id, 0), workerPartitionIndex)
          }

          override def onPushRecv(id: Id, deltaUpdate: P, ps: ParameterServer[Id, P, PSOut]): WOut = {
            params.put(id, params.getOrElseUpdate(id, 0) + deltaUpdate)
          }

          override def close(ps: ParameterServer[T, P, (T, P)]): WOut = {
            params.foreach(ps.output)
          }
        }, {
          case WorkerToPS(_, msg) => msg match {
            case Left(Pull(id)) => id.substring(2).toInt % psParallelism
            case Right(Push(id, _)) => id.substring(2).toInt % psParallelism
          }
        }, {
          case PSToWorker(wIdx, _) => wIdx
        },
        workerParallelism, psParallelism, iterWaitTime
      )

    outputDS.addSink(new RichSinkFunction[Either[WOut, PSOut]] {
      val result = new ArrayBuffer[PSOut]

      override def invoke(value: Either[WOut, PSOut]): Unit = {
        value match {
          case Right(param) => result.append(param)
          case _ => throw new IllegalStateException()
        }
      }

      override def close(): Unit = {
        throw new SuccessException[Seq[PSOut]](result)
      }
    }).setParallelism(1)

    FlinkTestUtils.executeWithSuccessCheck[Seq[PSOut]](env) {
      result =>
        result.sorted should be(expectedOutModel)
    }

  }

}
