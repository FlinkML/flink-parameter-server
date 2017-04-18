package hu.sztaki.ilab.ps.client.sender

import hu.sztaki.ilab.ps.common.CountLogic
import hu.sztaki.ilab.ps.entities.WorkerToPS

case class CountClientSender[P](max: Int)
  extends CountLogic[WorkerToPS[P]](max)
    with Serializable