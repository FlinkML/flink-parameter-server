package hu.sztaki.ilab.ps.sketch.minhash.pslogic

import hu.sztaki.ilab.ps.{LooseParameterServerLogic, ParameterServer, ParameterServerLogic}

import scala.collection.mutable

class MinHashPredictPSLogic(numHashes: Int, K: Int)
  extends ParameterServerLogic[Int, Either[(Int, Array[Long]), Array[Long]], (Int, Array[(Double, Int)])]{


  val model = new mutable.HashMap[Int, Array[Long]]()

  override def onPullRecv(id: Int,
                          workerPartitionIndex: Int,
                          ps: ParameterServer[Int, Either[(Int, Array[Long]), Array[Long]], (Int, Array[(Double, Int)])]): Unit =
    ps.answerPull(id, Right(model.getOrElseUpdate(id, Array.emptyLongArray)), workerPartitionIndex)


  override def onPushRecv(id: Int,
                          deltaUpdate: Either[(Int, Array[Long]), Array[Long]],
                          ps: ParameterServer[Int, Either[(Int, Array[Long]), Array[Long]], (Int, Array[(Double, Int)])]): Unit =
    deltaUpdate match {
      case Left((queryId, targetVector)) =>
        val topK = new mutable.ArrayBuffer[(Double, Int)]
        for ((k, v) <- model) {
          topK += ((calculateIntersect(targetVector, v), k))
        }
        ps.output((queryId, topK.toArray))
      case Right(newVector) =>
        model += ((id, newVector))
    }

  private def calculateIntersect(targetVector: Array[Long], v: Array[Long]): Double =
    if (targetVector.nonEmpty && v.nonEmpty) {
      (targetVector.zip(v) count { case (a, b) => a == b }).toDouble / numHashes
    } else 0
}
