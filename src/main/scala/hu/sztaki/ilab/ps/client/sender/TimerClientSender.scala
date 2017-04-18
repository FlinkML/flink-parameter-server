package hu.sztaki.ilab.ps.client.sender

import hu.sztaki.ilab.ps.common.TimerLogic
import hu.sztaki.ilab.ps.entities.WorkerOut

import scala.concurrent.duration._

case class TimerClientSender[P](intervalLength: FiniteDuration)
  extends TimerLogic[WorkerOut[P]](intervalLength)
    with Serializable