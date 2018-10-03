package hu.sztaki.ilab.ps

/**
  * Client interface for the ParameterServer that the worker can use.
  * This can be used in [[WorkerLogic]].
  *
  * @tparam Id
  * Type of parameter identifiers.
  * @tparam P
  * Type of parameters.
  * @tparam WorkerOut
  * Type of worker output.
  */
trait ParameterServerClient[Id, P, WorkerOut] extends Serializable {

  def pull(id: Id): Unit

  def push(id: Id, deltaUpdate: P): Unit

  def output(out: WorkerOut): Unit

}
