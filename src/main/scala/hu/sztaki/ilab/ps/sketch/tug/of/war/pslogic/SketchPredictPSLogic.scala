package hu.sztaki.ilab.ps.sketch.tug.of.war.pslogic

import hu.sztaki.ilab.ps.sketch.utils.Utils.{Vector, dotProduct}
import hu.sztaki.ilab.ps.{ParameterServer, ParameterServerLogic}

import scala.collection.mutable

class SketchPredictPSLogic(numHashes: Int, numMeans: Int, K: Int)
  extends ParameterServerLogic[Either[(Int, Vector), Vector], (Int, Array[(Double, Int)])]{

  val model = new mutable.HashMap[Int,Vector]()

  override def onPullRecv(id: Int,
                          workerPartitionIndex: Int,
                          ps: ParameterServer[Either[(Int, Vector), Vector], (Int, Array[(Double, Int)])]): Unit = {
    ps.answerPull(id, Left((0, model.getOrElseUpdate(id, new Vector(numHashes)))), workerPartitionIndex)
  }

  override def onPushRecv(id: Int,
                          deltaUpdate: Either[(Int, Vector), Vector],
                          ps: ParameterServer[Either[(Int, Vector), Vector], (Int, Array[(Double, Int)])]): Unit = {
    deltaUpdate match {
      case Left((queryId, targetVector)) =>
        val topK = new mutable.ArrayBuffer[(Double, Int)]
        val size: Int = math.ceil(numHashes / numMeans).toInt
        val targetSlices = (for(i <- 0 until numHashes by size) yield  targetVector.slice(i, i+size)).toArray
        for((k,v) <- model){

          val slices = (for(i <- 0 until numHashes by size) yield v.slice(i, i + size)).toArray.zip(targetSlices)
          val means = slices.map(x =>
            dotProduct(x._1, x._2).toDouble / x._1.length).sortWith(_ > _)

          val len = means.length

          if(means.length % 2 == 0)
              topK += (((means(len/2) + means(len/2 - 1)) / 2, k))
          else
              topK += ((means(math.floor(len/2).toInt), k))
        }
        ps.output((queryId, topK.sorted.takeRight(K).toArray))

      case Right(newVector) =>
        model.update(id, newVector)
    }
  }
}
