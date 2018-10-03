package hu.sztaki.ilab.ps.client.sender

import hu.sztaki.ilab.ps.common.TimerLogic
import hu.sztaki.ilab.ps.entities.WorkerToPS

import scala.concurrent.duration._

case class TimerClientSender[Id, P](intervalLength: FiniteDuration)
  extends TimerLogic[WorkerToPS[Id, P]](intervalLength)
    with Serializable