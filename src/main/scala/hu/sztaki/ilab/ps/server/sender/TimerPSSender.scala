package hu.sztaki.ilab.ps.server.sender

import hu.sztaki.ilab.ps.common.TimerLogic
import hu.sztaki.ilab.ps.entities.WorkerIn

import scala.concurrent.duration.FiniteDuration

case class TimerPSSender[P](intervalLength: FiniteDuration)
  extends TimerLogic[WorkerIn[P]](intervalLength)
    with Serializable