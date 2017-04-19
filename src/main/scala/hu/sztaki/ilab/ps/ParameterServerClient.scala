package hu.sztaki.ilab.ps

/**
  * Client interface for the ParameterServer that the worker can use.
  * This can be used in [[WorkerLogic]].
  *
  * @tparam P
  * Type of parameters.
  * @tparam WorkerOut
  * Type of worker output.
  */
trait ParameterServerClient[P, WorkerOut] extends Serializable {

  def pull(id: Int): Unit

  def push(id: Int, deltaUpdate: P): Unit

  def output(out: WorkerOut): Unit

}
