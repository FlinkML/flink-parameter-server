package hu.sztaki.ilab.ps.client.sender

import hu.sztaki.ilab.ps.common.CountLogic
import hu.sztaki.ilab.ps.entities.WorkerOut

case class CountClientSender[P](max: Int)
  extends CountLogic[WorkerOut[P]](max)
    with Serializable