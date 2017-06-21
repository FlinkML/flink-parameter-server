package hu.sztaki.ilab.ps.matrix.factorization

import hu.sztaki.ilab.ps.matrix.factorization.PsOnlineMatrixFactorization.Vector
import hu.sztaki.ilab.ps.matrix.factorization.Utils.{ItemId, UserId}
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * Created by bdaniel on 2017.04.30..
  */
class PSOnlineMatrixFactorizationImplicitTest {

}

object PSOnlineMatrixFactorizationImplicitTest{
  type Rating = (UserId, ItemId, Double)

  val milisecBetweenRatings = 1000
  val numIterations = 10

  val numFactors = 10
  val learningRate = 0.01
  val pullLimit = 100
  val workerParallelism = 4
  val psParallelism = 3
  val iterationWaitTime = 10000

  def main(args: Array[String]): Unit = {

    val input_file_name = args(0)
    val userVector_output_name = args(1)
    val itemVector_output_name = args(2)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.readTextFile(input_file_name)

    val lastFM_RichFlatMap = data.flatMap(new RichFlatMapFunction[String, (Int, Int, Double)] {
      var count = 0
      override def flatMap(value: String, out: Collector[(ItemId, ItemId, Double)]): Unit = {
        val fieldsArray = value.split(" ")
        val r = Tuple3(fieldsArray(1).toInt, fieldsArray(2).toInt, 1.0)
        out.collect(r)

        for(i <- 1 to 40){
         out.collect(r._1, 1+Random.nextInt(37988) , 0.0)
        }
      }
    })


    PsOnlineMatrixFactorization.psOnlineMF(
      lastFM_RichFlatMap,
      numFactors,
      learningRate,
      pullLimit,
      workerParallelism,
      psParallelism,
      iterationWaitTime)
        .addSink(new RichSinkFunction[Either[(UserId, Vector), (ItemId, Vector)]] {

          val userVectors = new mutable.HashMap[UserId, Array[Double]]
          val itemVectors = new mutable.HashMap[ItemId, Array[Double]]

          override def invoke(value: Either[(UserId, Vector), (ItemId, Vector)]): Unit = {

            value match {
              case Left((userId, vec)) =>
                userVectors.update(userId, vec)
              case Right((itemId, vec)) =>
                itemVectors.update(itemId, vec)
            }
          }

          override def close(): Unit = {
            val userVectorFile = new java.io.PrintWriter(new java.io.File(userVector_output_name))
            for((k,v) <- userVectors){
              for(value <- v){
                userVectorFile.write(k + ";" + value + '\n')
              }
            }
            userVectorFile.close()

            val itemVectorFile = new java.io.PrintWriter(new java.io.File(itemVector_output_name))
            for((k,v) <- itemVectors){
              for(value <- v){
                itemVectorFile.write(k + ";" + value + '\n')
              }
            }
            itemVectorFile.close()
          }

        }).setParallelism(1)

    env.execute()
  }
}
