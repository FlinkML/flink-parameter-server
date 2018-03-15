package hu.sztaki.ilab.ps.sketch.bloom.filter.experiments

import hu.sztaki.ilab.ps.sketch.bloom.filter.BloomFilterPredict
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable

/**
  * Basic experiment for predicting common occurrence.
  */
object BloomFilterPredictExp {

  def main(args: Array[String]): Unit = {

    val modelFile = args(0)
    val wordsInModel = args(1)
    val searchWords = args(2)
    val predictionFile = args(3)
    val workerParallelism = args(4).toInt
    val psParallelism = args(5).toInt
    val iterationWaitTime = args(6).toLong
    val pullLimit = args(7).toInt
    val numHashes = args(8).toInt
    val arraySize = args(9).toInt
    val K = args(10).toInt

    val wordList = scala.io.Source.fromFile(wordsInModel)
      .getLines
      .map(_.toLowerCase())
      .toList

    val hashToWord: Map[Int, String] = wordList.map(word => word.hashCode -> word).toMap

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val model: DataStream[(Int, Either[(Int, mutable.BitSet), mutable.BitSet])] = env
      .readTextFile(modelFile)
      .map(line => {
        val id = line.split(":")(0).toInt
        val params = line.split(":")(1).split(",").map(_.toInt)
        val bitSet = mutable.BitSet.empty
        for(elem <- params){
          bitSet += elem
        }
        (id, Right(bitSet))
      })

    val src = env
      .readTextFile(searchWords)
      .map(word => {
        (word.toLowerCase().hashCode, word.toLowerCase())
      })

    BloomFilterPredict
      .predict(src, model, arraySize, numHashes, K, workerParallelism, psParallelism, pullLimit, iterationWaitTime)
      .map(value => {
        var formattedOutput = s"${hashToWord(value._1)} - (${hashToWord(value._2.head._2)},${math.round(value._2.head._1)})"
        for ((score, id) <- value._2.tail) {
          formattedOutput += s", (${hashToWord(id)},${math.round(score)})"
        }
        formattedOutput
      }).setParallelism(psParallelism)
      .writeAsText(predictionFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1)

    env.execute()
  }

}
