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
  * @tparam P
  * Type of parameters.
  * @tparam WOut
  * Type of worker output.
  */
trait WorkerLogic[T, P, WOut] extends Serializable {

  /**
    * Method called when new data arrives.
    *
    * @param data
    * New data.
    * @param ps
    * Interface to ParameterServer.
    */
  def onRecv(data: T, ps: ParameterServerClient[P, WOut]): Unit

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
  def onPullRecv(paramId: Int, paramValue: P, ps: ParameterServerClient[P, WOut]): Unit

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
    * @tparam P
    * Type of parameters.
    * @tparam WOut
    * Type of worker output.
    * @return
    * [[WorkerLogic]] that limits pulls.
    */
  def addBlockingPullLimiter[T, P, WOut, WLogic <: WorkerLogic[T, P, WOut]](workerLogic: WLogic,
                                                                            pullLimit: Int): WorkerLogic[T, P, WOut] = {
    new WorkerLogic[T, P, WOut] {

      private var pullCounter = 0

      val psLock = new ReentrantLock()
      val canPull: Condition = psLock.newCondition()

      val wrappedPS = new ParameterServerClient[P, WOut] {

        private var ps: ParameterServerClient[P, WOut] = _

        def setPS(ps: ParameterServerClient[P, WOut]): Unit = {
          psLock.lock()
          try {
            this.ps = ps
          } finally {
            psLock.unlock()
          }
        }

        override def pull(id: Int): Unit = {
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

        override def push(id: Int, deltaUpdate: P): Unit = {
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

      override def onRecv(data: T, ps: ParameterServerClient[P, WOut]): Unit = {
        wrappedPS.setPS(ps)
        workerLogic.onRecv(data, wrappedPS)
      }

      override def onPullRecv(paramId: Int,
                              paramValue: P,
                              ps: ParameterServerClient[P, WOut]): Unit = {
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
    * @tparam P
    * Type of parameters.
    * @tparam WOut
    * Type of worker output.
    * @return
    * [[WorkerLogic]] that limits pulls.
    */
  def addPullLimiter[T, P, WOut](workerLogic: WorkerLogic[T, P, WOut],
                                 pullLimit: Int): WorkerLogic[T, P, WOut] = {
    new WorkerLogic[T, P, WOut] {

      private var pullCounter = 0
      private val pullQueue = mutable.Queue[Int]()

      val wrappedPS = new ParameterServerClient[P, WOut] {

        private var ps: ParameterServerClient[P, WOut] = _

        def setPS(ps: ParameterServerClient[P, WOut]): Unit = {
          this.ps = ps
        }

        override def pull(id: Int): Unit = {
          if (pullCounter < pullLimit) {
            pullCounter += 1
            ps.pull(id)
          } else {
            pullQueue.enqueue(id)
          }
        }

        override def push(id: Int, deltaUpdate: P): Unit = {
          ps.push(id, deltaUpdate)
        }

        override def output(out: WOut): Unit = {
          ps.output(out)
        }
      }

      override def onRecv(data: T, ps: ParameterServerClient[P, WOut]): Unit = {
        wrappedPS.setPS(ps)
        workerLogic.onRecv(data, wrappedPS)
      }

      override def onPullRecv(paramId: Int,
                              paramValue: P,
                              ps: ParameterServerClient[P, WOut]): Unit = {
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

/**
  * Somewhat simplified worker logic with pulls returning [[Future]].
  *
  * @tparam T
  * Type of incoming data.
  * @tparam P
  * Type of parameters.
  * @tparam WOut
  * Type of worker output.
  */
trait WorkerLogicWithFuture[T, P, WOut] extends WorkerLogic[T, P, WOut] {

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
  def onDataRecv(data: T, ps: PSClientWithFuture[P, WOut]): Unit

  private val pullWaiter = mutable.HashMap[Int, mutable.Queue[PullAnswerFuture[P]]]()

  private val psClient = new PSClientWithFuture[P, WOut] {

    private var ps: ParameterServerClient[P, WOut] = _

    def setPS(ps: ParameterServerClient[P, WOut]): Unit = {
      this.ps = ps
    }

    override def pull(id: Int): Future[(Int, P)] = {
      val pullAnswerFuture = PullAnswerFuture[P](id)
      pullWaiter.getOrElseUpdate(id, mutable.Queue.empty)
        .enqueue(pullAnswerFuture)
      pullAnswerFuture
    }

    override def push(id: Int, deltaUpdate: P): Unit = {
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
  override def onRecv(data: T, ps: ParameterServerClient[P, WOut]): Unit = {
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
  override def onPullRecv(paramId: Int, paramValue: P, ps: ParameterServerClient[P, WOut]): Unit = {
    pullWaiter(paramId).dequeue().pullArrived(paramId, paramValue)
  }

}

trait PSClientWithFuture[P, WorkerOut] extends Serializable {
  def pull(id: Int): Future[(Int, P)]

  def push(id: Int, deltaUpdate: P): Unit

  def output(out: WorkerOut): Unit
}

case class PullAnswerFuture[P](paramId: Int) extends Future[(Int, P)] {

  private var callback: Try[(Int, P)] => Any = x => ()
  private var pullAnswer: Option[Try[(Int, P)]] = None

  def pullArrived(paramId: Int, param: P): Unit = {
    pullAnswer = Some(Success(paramId -> param))
    callback(pullAnswer.get)
  }

  override def onComplete[U](f: (Try[(Int, P)]) => U)(implicit executor: ExecutionContext): Unit = {
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

  override def value: Option[Try[(Int, P)]] = {
    pullAnswer
  }

  override def ready(atMost: Duration)(implicit permit: CanAwait): PullAnswerFuture.this.type = {
    this
  }

  override def result(atMost: Duration)(implicit permit: CanAwait): (Int, P) = {
    throw new UnsupportedOperationException()
  }

}
