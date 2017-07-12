package hu.sztaki.ilab.ps.matrix.factorization

import hu.sztaki.ilab.ps.matrix.factorization.Utils.{ItemId, UserId}
import hu.sztaki.ilab.ps.matrix.factorization.factors.{RangedRandomFactorInitializerDescriptor, SGDUpdater}
import hu.sztaki.ilab.ps.server.SimplePSLogic
import hu.sztaki.ilab.ps.{FlinkParameterServer, ParameterServerClient, WorkerLogic}
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by bdaniel on 2017.04.21..
  */
class PsOnlineMatrixFactorization {
}

object PsOnlineMatrixFactorization{

  // A user rates an item with a Double rating
  type Rating = (UserId, ItemId, Double)
  type Vector = Array[Double]


  def psOnlineMF(src: DataStream[Rating],
                  numFactors: Int,
                  learningRate: Double,
                  minRange: Double,
                  maxRange: Double,
                  pullLimit: Int,
                  workerParallelism: Int,
                  psParallelism: Int,
                  iterationWaitTime: Long): DataStream[Either[(UserId, Vector), (ItemId, Vector)]] = {

    // initialization method and update method
    val factorInitDesc = RangedRandomFactorInitializerDescriptor(numFactors, minRange, maxRange)

    val factorUpdate = new SGDUpdater(learningRate)

    val workerLogic = new WorkerLogic[Rating, Vector, (UserId, Vector)] {

      val UserVector = new scala.collection.mutable.HashMap[UserId, Vector]
      val ratingBuffer = new mutable.HashMap[ItemId, ListBuffer[Rating]]

      override
      def close(): Unit = super.close()

      override
      def onPullRecv(paramId: ItemId, paramValue: Vector, ps: ParameterServerClient[Vector, (UserId, Vector)]): Unit = {
        val rate = ratingBuffer(paramId).head //ListQueue poll!!
        ratingBuffer(paramId).remove(0)

        val user = UserVector.getOrElseUpdate(rate._1, factorInitDesc.open().nextFactor(rate._1))
        val item = paramValue
        val (userDelta, itemDelta) = factorUpdate.delta(rate._3, user, item)

        UserVector(rate._1) = user.zip(userDelta).map(r => r._1 + r._2)

        ps.output(rate._1, UserVector(rate._1))
        ps.push(paramId, itemDelta)
      }


      override
      def onRecv(data: Rating, ps: ParameterServerClient[Vector, (UserId, Vector)]): Unit = {
        ratingBuffer.getOrElseUpdate(data._2, new ListBuffer[Rating]()) += data
        ps.pull(data._2) //ParameterServerTransform
      }
    }

    val serverLogic = new SimplePSLogic[Array[Double]](
      x => factorInitDesc.open().nextFactor(x), { (vec, deltaVec) => vec.zip(deltaVec).map(x => x._1 + x._2)}
    )

    val modelUpdates = FlinkParameterServer.transform(
      src,
      WorkerLogic.addPullLimiter(workerLogic, pullLimit),
      serverLogic,
      workerParallelism,
      psParallelism,
      iterationWaitTime)

    modelUpdates
  }
}
