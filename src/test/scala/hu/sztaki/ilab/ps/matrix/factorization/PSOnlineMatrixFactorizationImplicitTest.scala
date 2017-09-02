package hu.sztaki.ilab.ps.matrix.factorization

import hu.sztaki.ilab.ps.matrix.factorization.utils.Rating
import hu.sztaki.ilab.ps.matrix.factorization.utils.Utils.{ItemId, UserId}
import hu.sztaki.ilab.ps.matrix.factorization.utils.Vector._
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable


class PSOnlineMatrixFactorizationImplicitTest {

}

object PSOnlineMatrixFactorizationImplicitTest{

  val numFactors = 10
  val rangeMin = -0.1
  val rangeMax = 0.1
  val learningRate = 0.01
  val userMemory = 128
  val negativeSampleRate = 9
  val pullLimit = 1500
  val workerParallelism = 4
  val psParallelism = 4
  val iterationWaitTime = 10000

  def main(args: Array[String]): Unit = {

    val input_file_name = args(0)
    val userVector_output_name = args(1)
    val itemVector_output_name = args(2)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.readTextFile(input_file_name)

    val lastFM = data.flatMap(new RichFlatMapFunction[String, Rating] {

      override def flatMap(value: String, out: Collector[Rating]): Unit = {
        val fieldsArray = value.split(" ")
        val r = Rating.fromTuple(fieldsArray(1).toInt, fieldsArray(2).toInt, 1.0)
        out.collect(r)
      }
    })

    PSOnlineMatrixFactorization.psOnlineMF(
      lastFM,
      numFactors,
      rangeMin,
      rangeMax,
      learningRate,
      negativeSampleRate,
      userMemory,
      pullLimit,
      workerParallelism,
      psParallelism,
      iterationWaitTime)
        .addSink(new RichSinkFunction[Either[(UserId, Vector), (ItemId, Vector)]] {

          val userVectors = new mutable.HashMap[UserId, Vector]
          val itemVectors = new mutable.HashMap[ItemId, Vector]

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
