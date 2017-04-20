package hu.sztaki.ilab.ps

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
