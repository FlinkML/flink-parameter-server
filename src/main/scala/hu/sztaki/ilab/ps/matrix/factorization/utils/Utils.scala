package hu.sztaki.ilab.ps.matrix.factorization.utils

import hu.sztaki.ilab.ps.entities.{PSToWorker, Pull, Push, WorkerToPS}

import scala.collection.mutable

object Utils {


  /**
    * Identifier of users
    */
  type UserId = Int

  /**
    * Identifier of items
    */
  type ItemId = Int

  /**
    * Queue used for calculating TopK
    */
  type TopKQueue = mutable.PriorityQueue[(Double, ItemId)]

  /**
    * Initializes an empty queue for calculating TopK
    */
  def newTopKQueue(): TopKQueue = {
    new mutable.PriorityQueue[(Double, Int)]()(implicitly[Ordering[(Double, Int)]].reverse)
  }

  /**
    * Output type of TopK generator workers
    */
  type TopKWorkerOutput = (RichRating, TopKQueue)
}


object IDGenerator {
  private val n = new java.util.concurrent.atomic.AtomicLong

  /**
    * Generates a random rating ID
    */
  def next: Long = n.getAndIncrement()
}

/**
  * Partitioner used for communication between worker and server nodes
  */
class Partitioner[P](psParallelism: Int) extends Serializable{
  lazy val hashFunc: Any => Int = x => Math.abs(x.hashCode())

  lazy val workerToPSPartitioner: WorkerToPS[P] => Int = {
    case WorkerToPS(_, msg) =>
      msg match {
        case Left(Pull(pId)) => hashFunc(pId) % psParallelism
        case Right(Push(pId, _)) => hashFunc(pId) % psParallelism
      }
  }

  lazy val psToWorkerPartitioner: PSToWorker[P] => Int = {
    case PSToWorker(workerPartitionIndex, _) => workerPartitionIndex
  }
}


case class EOF() extends Serializable
