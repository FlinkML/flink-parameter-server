package hu.sztaki.ilab.ps.entities

case class WorkerToPS[P](workerPartitionIndex: Int, msg: Either[Pull, Push[P]])
case class PSToWorker[P](workerPartitionIndex: Int, msg: PullAnswer[P])

case class Pull(paramId: Int)
case class Push[P](paramId: Int, delta: P)
case class PullAnswer[P](paramId: Int, param: P)
