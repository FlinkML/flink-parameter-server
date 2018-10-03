package hu.sztaki.ilab.ps.server.sender

import hu.sztaki.ilab.ps.common.TimerLogic
import hu.sztaki.ilab.ps.entities.PSToWorker

import scala.concurrent.duration.FiniteDuration

case class TimerPSSender[Id, P](intervalLength: FiniteDuration)
  extends TimerLogic[PSToWorker[Id, P]](intervalLength)
    with Serializable