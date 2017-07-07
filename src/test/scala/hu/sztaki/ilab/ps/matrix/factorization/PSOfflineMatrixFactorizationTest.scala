package hu.sztaki.ilab.ps.matrix.factorization

import hu.sztaki.ilab.ps.matrix.factorization.Utils.{ItemId, UserId}
import hu.sztaki.ilab.ps.test.utils.FlinkTestUtils._
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.scalatest._
import prop._

import scala.collection.mutable
import scala.util.Random

object PSOfflineMatrixFactorizationTest {

  val numFactors = 15
  val numberOfRatings = 100
  val numUsers = 20
  val numItems = 15
  val random = new Random(47L)

  val ratings: Seq[(Int, Int, Double)] = Seq.fill(numberOfRatings)(
    (random.nextInt(numUsers), random.nextInt(numItems), random.nextDouble())
  )
    // eliminating duplicates
    .groupBy(x => (x._1, x._2)).mapValues(_.head).toSeq.map(_._2)

  def randomModelRMSE(numFactors: Int): Double = {
    val users = ratings.map(_._1).distinct.map((_, Array.fill(numFactors)(Random.nextDouble()))).toMap
    val items = ratings.map(_._2).distinct.map((_, Array.fill(numFactors)(Random.nextDouble()))).toMap

    computeRMSE(ratings, users, items)
  }

  def computeRMSE(rs: Iterable[(Int, Int, Double)],
                  users: collection.Map[Int, Array[Double]],
                  items: collection.Map[Int, Array[Double]]): Double = {
    def dot(xs: Array[Double], ys: Array[Double]): Double =
      xs.zip(ys).map { case (x, y) => x * y }.sum

    val sum = ratings.map {
      case (u, i, r) =>
        val diff = dot(users(u), items(i)) - r
        diff * diff
    }.sum

    Math.sqrt(sum / ratings.length)
  }
}

class PSOfflineMatrixFactorizationTest extends FlatSpec with PropertyChecks with Matchers {

  import PSOfflineMatrixFactorizationTest._

  "Offline MF with PS" should "give reasonable error on test data" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val src = env.fromCollection(ratings)

    import PSOfflineMatrixFactorization._
    PSOfflineMatrixFactorization.psOfflineMF(
      src,
      numFactors = numFactors,
      learningRate = 0.01,
      iterations = 10,
      minRange = -0.1,
      maxRange = 0.1,
      pullLimit = 10,
      workerParallelism = 4,
      psParallelism = 3,
      iterationWaitTime = 5000)
      .addSink(new RichSinkFunction[Either[(UserId, Vector), (ItemId, Vector)]] {

        private val userVecs = mutable.HashMap[UserId, Vector]()
        private val itemVecs = mutable.HashMap[UserId, Vector]()

        override def invoke(value: Either[(UserId, Vector), (ItemId, Vector)]): Unit = {
          value match {
            case Left((userId, vec)) =>
              userVecs.update(userId, vec)
            case Right((itemId, vec)) =>
              itemVecs.update(itemId, vec)
          }
        }

        override def close(): Unit = {
          // compute RMSE
          val rmse = computeRMSE(ratings, userVecs, itemVecs)
          throw SuccessException(rmse)
        }
      }).setParallelism(1)

    println(ratings.length)

    val maxAllowedRMSE = 0.5

    executeWithSuccessCheck[Double](env) {
      rmse =>
        if (rmse > maxAllowedRMSE) {
          fail(s"Got RMSE: $rmse, expected lower than $maxAllowedRMSE." +
            s" Note that the result highly depends on environment due to the asynchronous updates.")
        }
    }
  }
}
