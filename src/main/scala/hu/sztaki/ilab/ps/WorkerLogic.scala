package hu.sztaki.ilab.ps

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
