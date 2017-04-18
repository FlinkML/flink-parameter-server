package hu.sztaki.ilab.ps.entities

case class WorkerOut[P](partitionId: Int, msg: Either[Int, (Int, P)])
case class WorkerIn[P](id: Int, workerPartitionIndex: Int, msg: P)