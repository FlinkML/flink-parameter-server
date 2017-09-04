package hu.sztaki.ilab.ps.matrix.factorization

import java.io.{FileWriter, PrintWriter}

import hu.sztaki.ilab.ps.matrix.factorization.utils.Rating
import hu.sztaki.ilab.ps.matrix.factorization.utils.Utils.{ItemId, UserId}
import hu.sztaki.ilab.ps.matrix.factorization.utils.Vector.VectorLength
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._

class PSOnlineMatrixFactorizationAndTopKGeneratorTest {

}

object PSOnlineMatrixFactorizationAndTopKGeneratorTest {

  val numFactors = 10
  val rangeMin = -0.01
  val rangeMax = 0.01
  val learningRate = 0.4
  val userMemory = 4
  val K = 100
  val workerK = 50
  val bucketSize = 100
  val negativeSampleRate = 9
  val pullLimit = 1200
  val workerParallelism = 4
  val psParallelism = 4
  val iterationWaitTime = 20000

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val src = env.readTextFile("TestData/test_batch").map(line => {
      val fieldsArray = line.split(",")

      Rating(fieldsArray(1).toInt, fieldsArray(2).toInt, 1.0, fieldsArray(0).toLong)
    })

    PSOnlineMatrixFactorizationAndTopKGenerator.psOnlineLearnerAndGenerator(
      src,
      numFactors,
      rangeMin,
      rangeMax,
      learningRate,
      negativeSampleRate,
      userMemory,
      K,
      workerK,
      bucketSize,
      pullLimit = pullLimit,
      iterationWaitTime = iterationWaitTime).addSink(new RichSinkFunction[(UserId, ItemId, Long, List[(Double, ItemId)])] {

      var sumnDCG = 0.0
      var counter = 0
      var hit = 0

      val log2: VectorLength = Math.log(2)

      override def invoke(value: (UserId, ItemId, Long, List[(Double, ItemId)])): Unit = {

        val index = value._4.indexWhere (
          recommendation => recommendation._2 == value._2 ) match {

          case -1 => Int.MaxValue

          case i => i + 1
        }


        val nDCG = index match {

          case Int.MaxValue => 0.0

          case i => log2 / Math.log(1.0 + i)
        }

        if(nDCG != 0)
          hit += 1
        sumnDCG += nDCG
        counter += 1
      }

      override def close(): Unit = {
        val outputFile = new PrintWriter(new FileWriter("TestData/PSOnlineMatrixFactorizationAndTopKGenerator_nDCG.out"))

        val avgnDCG = sumnDCG / counter

        outputFile write s"nDCG: $avgnDCG \n"
        outputFile write s"hit: $hit \n"
        outputFile write s"invokes: $counter"

        outputFile close()
      }
    }).setParallelism(1)

    env.execute()
  }
}
