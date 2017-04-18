package hu.sztaki.ilab.ps.server.sender

import hu.sztaki.ilab.ps.PSSender
import hu.sztaki.ilab.ps.common.{Combinable, CombinationLogic}
import hu.sztaki.ilab.ps.entities.{PSToWorker, PullAnswer}

import scala.collection.mutable.ArrayBuffer

class CombinationPSSender[P](condition: (List[Combinable[PSToWorker[P]]]) => Boolean,
                             combinables: List[Combinable[PSToWorker[P]]])
  extends CombinationLogic[PSToWorker[P]](condition, combinables)
    with PSSender[Array[PSToWorker[P]], P]
    with Serializable {

  override def onPullAnswer(id: Int, value: P, workerPartitionIndex: Int, collectAnswerMsg: (Array[PSToWorker[P]]) => Unit): Unit = {
    logic(
      (array: ArrayBuffer[PSToWorker[P]]) => {
        array += PSToWorker(workerPartitionIndex, PullAnswer(id, value))
      },
      collectAnswerMsg
    )
  }

}
