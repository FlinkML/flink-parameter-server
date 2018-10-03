package hu.sztaki.ilab.ps.server

import hu.sztaki.ilab.ps.ParameterServer
import org.scalatest._
import org.scalatest.prop._

class LockPSLogicBTest extends FlatSpec with PropertyChecks with Matchers {
  type P = Int
  type PSOut = (Int, Int)

  "If a pull is not prevented by push it" should "throw exeption" in {
    val testPsLogic = new LockPSLogicB[P, P]((x: Int) => x, (x: P, y: P) => x)
    a[IllegalStateException] should be thrownBy {
      testPsLogic.onPushRecv(42, 42, new ParameterServer[P, P, PSOut] {
        override def answerPull(id: P, value: P, workerPartitionIndex: P): Unit = {}

        override def output(out: (P, P)): Unit = {}
      })
    }
  }

  "Model's state initilaization" should "be working" in {
    val testPsLogic = new LockPSLogicB[P, P]((x: Int) => 23, (x: P, y: P) => y)
    testPsLogic.onPullRecv(42, 42, new ParameterServer[P, P, PSOut] {
      override def answerPull(id: P, value: P, workerPartitionIndex: P): Unit = {}

      override def output(out: (P, P)): Unit = {}
    })
    testPsLogic.params(42)._2 should be (23)
  }

  "If a pull is prevented by initial a model it" should "be updated after a push" in {
    val testPsLogic = new LockPSLogicB[P, P]((x: Int) => 0, (x: P, y: P) => y)
    testPsLogic.onPullRecv(42, 42, new ParameterServer[P, P, PSOut] {
      override def answerPull(id: P, value: P, workerPartitionIndex: P): Unit = {}

      override def output(out: (P, P)): Unit = {}
    })
    val mockPS = new ParameterServer[P, P, PSOut] {
      var x = (0, 0)

      override def answerPull(id: P, value: P, workerPartitionIndex: P): Unit = {}

      override def output(out: (P, P)): Unit = {
        x = out
      }
    }
    testPsLogic.onPushRecv(42, 23, mockPS)
    mockPS.x should be(42, 23)
  }

  "The locking system" should "work" in {
    val testPsLogic = new LockPSLogicB[P, P]((x: Int) => 0, (x: P, y: P) => y)
    testPsLogic.onPullRecv(42, 42, new ParameterServer[P, P, PSOut] {
      override def answerPull(id: P, value: P, workerPartitionIndex: P): Unit = {}

      override def output(out: (P, P)): Unit = {}
    })
    val mockPS = new ParameterServer[P, P, PSOut] {
      var triggered = false

      override def answerPull(id: P, value: P, workerPartitionIndex: P): Unit = {
        triggered = true
      }

      override def output(out: (P, P)): Unit = {}
    }
    testPsLogic.onPullRecv(42, 43, mockPS)
    mockPS.triggered should be (false)
    val (isLocked, param, pullQueue) = testPsLogic.params(42)
    isLocked should be (true)
    param should be (0)
    pullQueue.size should be (1)
    pullQueue.head should be (43)
  }

  "Lock" should "be released" in {
    val testPsLogic = new LockPSLogicB[P, P]((x: Int) => 0, (x: P, y: P) => y)
    testPsLogic.onPullRecv(42, 42, new ParameterServer[P, P, PSOut] {
      override def answerPull(id: P, value: P, workerPartitionIndex: P): Unit = {}

      override def output(out: (P, P)): Unit = {}
    })
    testPsLogic.onPushRecv(42, 23, new ParameterServer[P, P, PSOut] {
      override def answerPull(id: P, value: P, workerPartitionIndex: P): Unit = {}

      override def output(out: (P, P)): Unit = {}
    })
    val (isLocked, param, pullQueue) = testPsLogic.params(42)
    isLocked should be (false)
    param should be (23)
    pullQueue.isEmpty should be (true)
  }

  "Lock" should "be hold and is should answer to the nexet request from queue" in {
    val testPsLogic = new LockPSLogicB[P, P]((x: Int) => 0, (x: P, y: P) => y)
    testPsLogic.onPullRecv(42, 42, new ParameterServer[P, P, PSOut] {
      override def answerPull(id: P, value: P, workerPartitionIndex: P): Unit = {}

      override def output(out: (P, P)): Unit = {}
    })
    testPsLogic.onPullRecv(42, 43, new ParameterServer[P, P, PSOut] {
      override def answerPull(id: P, value: P, workerPartitionIndex: P): Unit = {}

      override def output(out: (P, P)): Unit = {}
    })
    val mockPS = new ParameterServer[P, P, PSOut] {
      var x = (0, 0, 0)

      override def answerPull(id: P, value: P, workerPartitionIndex: P): Unit = {
        x = (id, value, workerPartitionIndex)
      }

      override def output(out: (P, P)): Unit = {}
    }
    testPsLogic.onPushRecv(42, 23, mockPS)
    mockPS.x should be (42, 23, 43)
    val (isLocked, param, pullQueue) = testPsLogic.params(42)
    isLocked should be (true)
    param should be (23)
    pullQueue.isEmpty should be (true)
  }

  "The pull queue" should "not be hold the same workerid duplicated" in {
    val testPsLogic = new LockPSLogicB[P, P]((x: Int) => 0, (x: P, y: P) => y)
    testPsLogic.onPullRecv(42, 42, new ParameterServer[P, P, PSOut] {
      override def answerPull(id: P, value: P, workerPartitionIndex: P): Unit = {}

      override def output(out: (P, P)): Unit = {}
    })
    testPsLogic.onPullRecv(42, 43, new ParameterServer[P, P, PSOut] {
      override def answerPull(id: P, value: P, workerPartitionIndex: P): Unit = {}

      override def output(out: (P, P)): Unit = {}
    })
    testPsLogic.onPullRecv(42, 43, new ParameterServer[P, P, PSOut] {
      override def answerPull(id: P, value: P, workerPartitionIndex: P): Unit = {}

      override def output(out: (P, P)): Unit = {}
    })
    val (isLocked, param, pullQueue) = testPsLogic.params(42)
    isLocked should be (true)
    param should be (0)
    pullQueue.size should be (1)
    pullQueue.head should be (43)
  }

  "The pull queue" should "not be hold the same workerid duplicated: tested with push together" in {
    val testPsLogic = new LockPSLogicB[P, P]((x: Int) => 0, (x: P, y: P) => y)
    testPsLogic.onPullRecv(42, 42, new ParameterServer[P, P, PSOut] {
      override def answerPull(id: P, value: P, workerPartitionIndex: P): Unit = {}

      override def output(out: (P, P)): Unit = {}
    })
    testPsLogic.onPullRecv(42, 43, new ParameterServer[P, P, PSOut] {
      override def answerPull(id: P, value: P, workerPartitionIndex: P): Unit = {}

      override def output(out: (P, P)): Unit = {}
    })
    testPsLogic.onPullRecv(42, 43, new ParameterServer[P, P, PSOut] {
      override def answerPull(id: P, value: P, workerPartitionIndex: P): Unit = {}

      override def output(out: (P, P)): Unit = {}
    })
    val mockPS = new ParameterServer[P, P, PSOut] {
      var x = (0, 0, 0)

      override def answerPull(id: P, value: P, workerPartitionIndex: P): Unit = {
        x = (id, value, workerPartitionIndex)
      }

      override def output(out: (P, P)): Unit = {}
    }
    testPsLogic.onPushRecv(42, 23, mockPS)
    mockPS.x should be (42, 23, 43)
    val (isLocked, param, pullQueue) = testPsLogic.params(42)
    isLocked should be (true)
    param should be (23)
    pullQueue.isEmpty should be (true)
  }

}