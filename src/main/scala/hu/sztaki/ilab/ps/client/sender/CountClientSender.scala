package hu.sztaki.ilab.ps.client.sender

import hu.sztaki.ilab.ps.common.CountLogic
import hu.sztaki.ilab.ps.entities.WorkerToPS

case class CountClientSender[Id, P](max: Int)
  extends CountLogic[WorkerToPS[Id, P]](max)
    with Serializable