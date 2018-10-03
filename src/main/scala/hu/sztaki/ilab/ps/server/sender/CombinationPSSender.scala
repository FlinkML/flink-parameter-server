package hu.sztaki.ilab.ps.server.sender

import hu.sztaki.ilab.ps.PSSender
import hu.sztaki.ilab.ps.common.{Combinable, CombinationLogic}
import hu.sztaki.ilab.ps.entities.{PSToWorker, PullAnswer}

import scala.collection.mutable.ArrayBuffer

class CombinationPSSender[Id, P](condition: (List[Combinable[PSToWorker[Id, P]]]) => Boolean,
                             combinables: List[Combinable[PSToWorker[Id, P]]])
  extends CombinationLogic[PSToWorker[Id, P]](condition, combinables)
    with PSSender[Array[PSToWorker[Id, P]], Id, P]
    with Serializable {

  override def onPullAnswer(id: Id, value: P, workerPartitionIndex: Int, collectAnswerMsg: (Array[PSToWorker[Id, P]]) => Unit): Unit = {
    logic(
      (array: ArrayBuffer[PSToWorker[Id, P]]) => {
        array += PSToWorker(workerPartitionIndex, PullAnswer(id, value))
      },
      collectAnswerMsg
    )
  }

}
