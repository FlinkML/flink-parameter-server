package hu.sztaki.ilab.ps.sketch.tug.of.war.pslogic

import hu.sztaki.ilab.ps.sketch.utils.Utils._
import hu.sztaki.ilab.ps.{ParameterServer, ParameterServerLogic}

import scala.collection.mutable

class SketchPSLogic(paramInit: => Int => Vector, paramUpdate: => (Vector,Vector) =>Vector) extends ParameterServerLogic[Either[Vector, (Long, Vector)], (Long, Array[(Long, Int)])]{

  val model = new mutable.HashMap[Int,Vector]()

  @transient lazy val init: (Int) => Vector = paramInit
  @transient lazy val update: (Vector, Vector) => Vector = paramUpdate

  override def onPullRecv(id: Int, workerPartitionIndex: Int,
                          ps: ParameterServer[Either[Vector, (Long, Vector)], (Long, Array[(Long, Int)])]): Unit = {
    ps.answerPull(id, Left(model.getOrElseUpdate(id, init(id))), workerPartitionIndex)
  }

  override def onPushRecv(id: Int, deltaUpdate: Either[Vector, (Long, Vector)],
                          ps: ParameterServer[Either[Vector, (Long, Vector)], (Long, Array[(Long, Int)])]): Unit = {
    deltaUpdate match {
      case Left(delta) =>
        val parameter = model.get(id) match {
          case Some(param) =>
            update(param, delta)
          case None =>
            delta
        }
        model += ((id, parameter))

      case Right((queryId, targetVector)) =>
        val topK = new mutable.ArrayBuffer[(Long, Int)]
        for((k,v) <- model){
          topK += ((dotProduct(targetVector, v), k))
        }
        ps.output((queryId, topK.sorted.takeRight(100).toArray))
    }
  }
}
