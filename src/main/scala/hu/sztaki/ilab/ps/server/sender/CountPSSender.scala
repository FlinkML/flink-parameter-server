package hu.sztaki.ilab.ps.server.sender

import hu.sztaki.ilab.ps.common.CountLogic
import hu.sztaki.ilab.ps.entities.WorkerIn

case class CountPSSender[P](max: Int)
  extends CountLogic[WorkerIn[P]](max)
    with Serializable