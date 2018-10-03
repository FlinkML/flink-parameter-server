package hu.sztaki.ilab.ps.entities

case class WorkerToPS[Id, P](workerPartitionIndex: Int, msg: Either[Pull[Id], Push[Id, P]])
case class PSToWorker[Id, P](workerPartitionIndex: Int, msg: PullAnswer[Id, P])

case class Pull[Id](paramId: Id)
case class Push[Id, P](paramId: Id, delta: P)
case class PullAnswer[Id, P](paramId: Id, param: P)
