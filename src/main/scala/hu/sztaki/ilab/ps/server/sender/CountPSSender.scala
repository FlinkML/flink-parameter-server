package hu.sztaki.ilab.ps.server.sender

import hu.sztaki.ilab.ps.common.CountLogic
import hu.sztaki.ilab.ps.entities.PSToWorker

case class CountPSSender[Id, P](max: Int)
  extends CountLogic[PSToWorker[Id, P]](max)
    with Serializable