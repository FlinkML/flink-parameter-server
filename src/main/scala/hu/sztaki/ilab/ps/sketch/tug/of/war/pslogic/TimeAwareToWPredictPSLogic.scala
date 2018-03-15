package hu.sztaki.ilab.ps.sketch.tug.of.war.pslogic

import hu.sztaki.ilab.ps.sketch.utils.Utils.{Vector, dotProduct}
import hu.sztaki.ilab.ps.{ParameterServer, ParameterServerLogic}

import scala.collection.mutable

class TimeAwareToWPredictPSLogic(numHashes: Int, numMeans: Int, K: Int)
  extends ParameterServerLogic[Either[((Int, Int), Vector),  (Int, Vector)], ((Int, Int), Array[(Double, Int)])]{

  val model = new mutable.HashMap[(Int, Int), Vector]()

  override def onPullRecv(id: Int, workerPartitionIndex: Int,
                          ps: ParameterServer[Either[((Int, Int), Vector), (Int, Vector)], ((Int, Int), Array[(Double, Int)])]): Unit = {
    model
      .filter(_._1._1 == id)
      .foreach(p => {
        ps.answerPull(id, Left(p._1, p._2), workerPartitionIndex)
      })
  }


  override def onPushRecv(id: Int, deltaUpdate: Either[((Int, Int), Vector), (Int, Vector)],
                          ps: ParameterServer[Either[((Int, Int), Vector), (Int, Vector)], ((Int, Int), Array[(Double, Int)])]): Unit = {
    deltaUpdate match {
      case Left(((queryId, timeSlot), targetVector)) =>
        val topK = new mutable.ArrayBuffer[(Double, Int)]

        val size: Int = math.ceil(numHashes / numMeans).toInt
        val targetSlices = (for(i <- 0 until numHashes by size) yield  targetVector.slice(i, i+size)).toArray

        val relevantVectors = model.filter(_._1._2 == timeSlot)

        for((k,v) <- relevantVectors){

          val slices = (for(i <- 0 until numHashes by size) yield v.slice(i, i + size)).toArray.zip(targetSlices)
          val means = slices.map(x =>
            dotProduct(x._1, x._2).toDouble / x._1.length).sortWith(_ > _)

          val len = means.length

          if(len % 2 == 0){
            topK += (((means(len/2) + means(len/2 - 1)) / 2, k._1))
          }
          else {
            topK += ((means(math.floor(len/2).toInt), k._1))
          }

        }

        ps.output(((queryId, timeSlot), topK.sorted.takeRight(K).toArray))

      case Right((timeSlot, newParam)) =>
        model.update((id, timeSlot), newParam)
    }
  }
}
