package hu.sztaki.ilab.ps.matrix.factorization

import hu.sztaki.ilab.ps.matrix.factorization.sink.nDCGSink
import hu.sztaki.ilab.ps.matrix.factorization.utils.InputTypes.Rating
import org.apache.flink.streaming.api.scala._

class PSOnlineMatrixFactorizationAndTopKGeneratorTest {

}

object PSOnlineMatrixFactorizationAndTopKGeneratorTest {

  val numFactors = 10
  val rangeMin = -0.01
  val rangeMax = 0.01
  val learningRate = 0.2
  val userMemory = 4
  val K = 100
  val workerK = 100
  val bucketSize = 100
  val negativeSampleRate = 9
  val pullLimit = 800
  val workerParallelism = 4
  val psParallelism = 4
  val iterationWaitTime = 20000

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val src = env.readTextFile("TestData/week_all").map(line => {
      val fieldsArray = line.split(",")

      Rating(fieldsArray(1).toInt, fieldsArray(2).toInt, 1.0, fieldsArray(0).toLong)
    })

    val topK = PSOnlineMatrixFactorizationAndTopKGenerator.psOnlineLearnerAndGenerator(
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
      iterationWaitTime = iterationWaitTime)

    nDCGSink.nDCGPeriodsToCsv(topK, "TestData/onlineMF_nDCG.csv", 86400)


    env.execute()
  }
}
