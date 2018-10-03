package hu.sztaki.ilab.ps

import java.util.concurrent.locks.{Condition, ReentrantLock}
import scala.collection.mutable
import scala.concurrent.{CanAwait, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Success, Try}

/**
  * Logic of the worker, that stores and processes the data.
  * To use the ParameterServer, this must be implemented.
  *
  * @tparam T
  * Type of incoming data.
  * @tparam Id
  * Type of parameter identifiers.
  * @tparam P
  * Type of parameters.
  * @tparam WOut
  * Type of worker output.
  */
trait WorkerLogic[T, Id, P, WOut] extends LooseWorkerLogic[T, Id, P, P, WOut]

/**
  * Logic of the worker, that stores and processes the data.
  * To use the ParameterServer, this must be implemented.
  *
  * @tparam T
  * Type of incoming data.
  * @tparam Id
  * Type of parameter identifiers.
  * @tparam PullP
  * Type of parameters for pull.
  * @tparam PushP
  * Type of parameters for push.
  * @tparam WOut
  * Type of worker output.
  */
trait LooseWorkerLogic[T, Id, PullP, PushP, WOut] extends Serializable {

  /**
    * Method called when the Flink operator is created
    */
  def open(): Unit ={}

  /**
    * Method called when new data arrives.
    *
    * @param data
    * New data.
    * @param ps
    * Interface to ParameterServer.
    */
  def onRecv(data: T, ps: ParameterServerClient[Id, PushP, WOut]): Unit

  /**
    * Method called when an answer arrives to a pull message.
    * It contains the parameter.
    *
    * @param paramId
    * Identifier of the received parameter.
    * @param paramValue
    * Value of the received parameter.
    * @param ps
    * Interface to ParameterServer.
    */
  def onPullRecv(paramId: Id, paramValue: PullP, ps: ParameterServerClient[Id, PushP, WOut]): Unit

  /**
    * Method called when processing is finished.
    */
  def close(): Unit = ()

}

object WorkerLogic {

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
    * @tparam Id
    * Type of parameter identifiers.
   * @tparam P
    * Type of parameters.
    * @tparam WOut
    * Type of worker output.
    * @return
    * [[WorkerLogic]] that limits pulls.
    */
  def addBlockingPullLimiter[T, Id, P, WOut, WLogic <: WorkerLogic[T, Id, P, WOut]](workerLogic: WLogic,
                                                                            pullLimit: Int): WorkerLogic[T, Id, P, WOut] = {
    new WorkerLogic[T, Id, P, WOut] {

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
    * @tparam Id
    * Type of parameter identifiers.
    * @tparam P
    * Type of parameters.
    * @tparam WOut
    * Type of worker output.
    * @return
    * [[WorkerLogic]] that limits pulls.
    */
  def addPullLimiter[T, Id, P, WOut](workerLogic: WorkerLogic[T, Id, P, WOut],
                                 pullLimit: Int): WorkerLogic[T, Id, P, WOut] = {
    new WorkerLogic[T, Id, P, WOut] {

      private var pullCounter = 0
      private val pullQueue = mutable.Queue[Id]()

      val wrappedPS = new ParameterServerClient[Id, P, WOut] {

        private var ps: ParameterServerClient[Id, P, WOut] = _

        def setPS(ps: ParameterServerClient[Id, P, WOut]): Unit = {
          this.ps = ps
        }

        override def pull(id: Id): Unit = {
          pullQueue synchronized {
            if (pullCounter < pullLimit) {
              pullCounter += 1
              ps.pull(id)
            } else {
              pullQueue.enqueue(id)
            }
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
        pullQueue synchronized {
          pullCounter -= 1
          if (pullQueue.nonEmpty) {
            val id = pullQueue.dequeue()
            wrappedPS.pull(id)
          }
        }
      }
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
    * @tparam Id
    * Type of parameter identifiers.
    * @tparam PullP
    * Type of Pull parameters.
    * @tparam PushP
    * Type of Push parameters.
    * @tparam WOut
    * Type of worker output.
    * @return
    * [[WorkerLogic]] that limits pulls.
    */
  def addPullLimiter[T, Id, PullP, PushP, WOut](workerLogic: LooseWorkerLogic[T, Id, PullP, PushP, WOut],
                                 pullLimit: Int): LooseWorkerLogic[T, Id, PullP, PushP, WOut] =
    new LooseWorkerLogic[T, Id, PullP, PushP, WOut] {

      private var pullCounter = 0
      private val pullQueue = mutable.Queue[Id]()

      val wrappedPS = new ParameterServerClient[Id, PushP, WOut] {

        private var ps: ParameterServerClient[Id, PushP, WOut] = _

        def setPS(ps: ParameterServerClient[Id, PushP, WOut]): Unit = {
          this.ps = ps
        }

        override def pull(id: Id): Unit = {
          pullQueue synchronized {
            if (pullCounter < pullLimit) {
              pullCounter += 1
              ps.pull(id)
            } else {
              pullQueue.enqueue(id)
            }
          }
        }

        override def push(id: Id, deltaUpdate: PushP): Unit = {
          ps.push(id, deltaUpdate)
        }

        override def output(out: WOut): Unit = {
          ps.output(out)
        }
      }

      override def onRecv(data: T, ps: ParameterServerClient[Id, PushP, WOut]): Unit = {
        wrappedPS.setPS(ps)
        workerLogic.onRecv(data, wrappedPS)
      }

      override def onPullRecv(paramId: Id,
                              paramValue: PullP,
                              ps: ParameterServerClient[Id, PushP, WOut]): Unit = {
        wrappedPS.setPS(ps)
        workerLogic.onPullRecv(paramId, paramValue, wrappedPS)
        pullQueue synchronized {
          pullCounter -= 1
          if (pullQueue.nonEmpty) {
            val id = pullQueue.dequeue()
            wrappedPS.pull(id)
          }
        }
      }
    }
}

/**
  * Somewhat simplified worker logic with pulls returning [[Future]].
  *
  * @tparam T
  * Type of incoming data.
  * @tparam Id
  * Type of parameter identifiers.
  * @tparam P
  * Type of parameters.
  * @tparam WOut
  * Type of worker output.
  */
trait WorkerLogicWithFuture[T, Id, P, WOut] extends WorkerLogic[T, Id, P, WOut] {

  // TODO test performance (memory and computation time) against simple implementation
  // TODO test

  /**
    * Method called when new data arrives.
    *
    * @param data
    * New data.
    * @param ps
    * Interface to ParameterServer with [[Future]] in pulls.
    */
  def onDataRecv(data: T, ps: PSClientWithFuture[Id, P, WOut]): Unit

  private val pullWaiter = mutable.HashMap[Id, mutable.Queue[PullAnswerFuture[Id, P]]]()

  private val psClient = new PSClientWithFuture[Id, P, WOut] {

    private var ps: ParameterServerClient[Id, P, WOut] = _

    def setPS(ps: ParameterServerClient[Id, P, WOut]): Unit = {
      this.ps = ps
    }

    override def pull(id: Id): Future[(Id, P)] = {
      val pullAnswerFuture = PullAnswerFuture[Id, P](id)
      pullWaiter.getOrElseUpdate(id, mutable.Queue.empty)
        .enqueue(pullAnswerFuture)
      pullAnswerFuture
    }

    override def push(id: Id, deltaUpdate: P): Unit = {
      ps.push(id, deltaUpdate)
    }

    override def output(out: WOut): Unit = {
      ps.output(out)
    }
  }

  /**
    * Method called when new data arrives.
    *
    * @param data
    * New data.
    * @param ps
    * Interface to ParameterServer.
    */
  override def onRecv(data: T, ps: ParameterServerClient[Id, P, WOut]): Unit = {
    psClient.setPS(ps)
    onDataRecv(data, psClient)
  }

  /**
    * Method called when an answer arrives to a pull message.
    * It contains the parameter.
    *
    * @param paramId
    * Identifier of the received parameter.
    * @param paramValue
    * Value of the received parameter.
    * @param ps
    * Interface to ParameterServer.
    */
  override def onPullRecv(paramId: Id, paramValue: P, ps: ParameterServerClient[Id, P, WOut]): Unit = {
    pullWaiter(paramId).dequeue().pullArrived(paramId, paramValue)
  }

}

trait PSClientWithFuture[Id, P, WorkerOut] extends Serializable {
  def pull(id: Id): Future[(Id, P)]

  def push(id: Id, deltaUpdate: P): Unit

  def output(out: WorkerOut): Unit
}

case class PullAnswerFuture[Id, P](paramId: Id) extends Future[(Id, P)] {

  private var callback: Try[(Id, P)] => Any = x => ()
  private var pullAnswer: Option[Try[(Id, P)]] = None

  def pullArrived(paramId: Id, param: P): Unit = {
    pullAnswer = Some(Success(paramId -> param))
    callback(pullAnswer.get)
  }

  override def onComplete[U](f: (Try[(Id, P)]) => U)(implicit executor: ExecutionContext): Unit = {
    pullAnswer match {
      case None =>
        callback = x => {
          callback(x)
          f(x)
        }
      case Some(ans) =>
        f(ans)
    }
  }

  override def isCompleted: Boolean = {
    pullAnswer.isDefined
  }

  override def value: Option[Try[(Id, P)]] = {
    pullAnswer
  }

  override def ready(atMost: Duration)(implicit permit: CanAwait): PullAnswerFuture.this.type = {
    this
  }

  override def result(atMost: Duration)(implicit permit: CanAwait): (Id, P) = {
    throw new UnsupportedOperationException()
  }

}
