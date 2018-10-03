package hu.sztaki.ilab.ps.matrix.factorization.workers

import java.util.concurrent.locks.{Condition, ReentrantLock}
import hu.sztaki.ilab.ps.{ParameterServerClient, WorkerLogic}
import scala.collection.mutable


trait BaseMFWorkerLogic[T, Id, P, WOut] extends WorkerLogic[T, Id, P, WOut]{

  val model = new mutable.HashMap[Id, P]

  def updateModel(id: Id, param: P)

}

object BaseMFWorkerLogic {


  /**
    * Adds a pull limiter to a [[WorkerLogic]].
    * If there are more unanswered pulls by a worker than the pull limit,
    * the pulling is blocked until pull answers arrive.
    *
    * Thus, worker must do the pulling in another thread in order to avoid deadlock.
    *
    * @param workerLogic
    * User defined [[WorkerLogic]]
    * @param pullLimit
    * Limit of unanswered pulls at a worker instance.
    * @tparam T
    * Type of training data.
    * @tparam P
    * Type of parameters.
    * @tparam WOut
    * Type of worker output.
    * @return
    * [[WorkerLogic]] that limits pulls.
    */
  def addBlockingPullLimiter[T, Id, P, WOut, WLogic <: BaseMFWorkerLogic[T, Id, P, WOut]](workerLogic: WLogic,
                                                                                  pullLimit: Int): BaseMFWorkerLogic[T, Id, P, WOut] = {
    new BaseMFWorkerLogic[T, Id, P, WOut] {


      private var pullCounter = 0

      val psLock = new ReentrantLock()
      val canPull: Condition = psLock.newCondition()

      val wrappedPS = new ParameterServerClient[Id, P, WOut] {

        private var ps: ParameterServerClient[Id, P, WOut] = _

        def setPS(ps: ParameterServerClient[Id, P, WOut]): Unit = {
          psLock.lock()
          try {
            this.ps = ps
          } finally {
            psLock.unlock()
          }
        }

        override def pull(id: Id): Unit = {
          psLock.lock()
          try {
            while (pullCounter >= pullLimit) {
              canPull.await()
            }

            pullCounter += 1
            ps.pull(id)
          } finally {
            psLock.unlock()
          }
        }

        override def push(id: Id, deltaUpdate: P): Unit = {
          psLock.lock()
          try {
            ps.push(id, deltaUpdate)
          } finally {
            psLock.unlock()
          }
        }

        override def output(out: WOut): Unit = {
          psLock.lock()
          try {
            ps.output(out)
          } finally {
            psLock.unlock()
          }
        }
      }

      override def onRecv(data: T, ps: ParameterServerClient[Id, P, WOut]): Unit = {
        wrappedPS.setPS(ps)
        workerLogic.onRecv(data, wrappedPS)
      }

      override def onPullRecv(paramId: Id,
                              paramValue: P,
                              ps: ParameterServerClient[Id, P, WOut]): Unit = {
        wrappedPS.setPS(ps)
        workerLogic.onPullRecv(paramId, paramValue, wrappedPS)
        psLock.lock()
        try {
          pullCounter -= 1
          canPull.signal()
        } finally {
          psLock.unlock()
        }
      }

      override def updateModel(id: Id, param: P): Unit = workerLogic.updateModel(id, param)
    }
  }

  /**
    * Adds a pull limiter to a [[WorkerLogic]].
    * If there are more unanswered pulls by a worker than the pull limit,
    * the pulls get buffered until pull answers arrive.
    *
    * @param workerLogic
    * User defined [[WorkerLogic]]
    * @param pullLimit
    * Limit of unanswered pulls at a worker instance.
    * @tparam T
    * Type of training data.
    * @tparam P
    * Type of parameters.
    * @tparam WOut
    * Type of worker output.
    * @return
    * [[WorkerLogic]] that limits pulls.
    */
  def addPullLimiter[T, Id, P, WOut](workerLogic: BaseMFWorkerLogic[T, Id, P, WOut],
                                 pullLimit: Int): BaseMFWorkerLogic[T, Id, P, WOut] = {
    new BaseMFWorkerLogic[T, Id, P, WOut] {

      override def updateModel(id: Id, param: P): Unit = workerLogic.updateModel(id, param)

      private var pullCounter = 0
      private val pullQueue = mutable.Queue[Id]()

      val wrappedPS = new ParameterServerClient[Id, P, WOut] {

        private var ps: ParameterServerClient[Id, P, WOut] = _

        def setPS(ps: ParameterServerClient[Id, P, WOut]): Unit = {
          this.ps = ps
        }

        override def pull(id: Id): Unit = {
          if (pullCounter < pullLimit) {
            pullCounter += 1
            ps.pull(id)
          } else {
            pullQueue.enqueue(id)
          }
        }

        override def push(id: Id, deltaUpdate: P): Unit = {
          ps.push(id, deltaUpdate)
        }

        override def output(out: WOut): Unit = {
          ps.output(out)
        }
      }

      override def onRecv(data: T, ps: ParameterServerClient[Id, P, WOut]): Unit = {
        wrappedPS.setPS(ps)
        workerLogic.onRecv(data, wrappedPS)
      }

      override def onPullRecv(paramId: Id,
                              paramValue: P,
                              ps: ParameterServerClient[Id, P, WOut]): Unit = {
        wrappedPS.setPS(ps)
        workerLogic.onPullRecv(paramId, paramValue, wrappedPS)
        pullCounter -= 1
        if (pullQueue.nonEmpty) {
          wrappedPS.pull(pullQueue.dequeue())
        }
      }
    }
  }
}
