package hu.sztaki.ilab.ps.sketch.minhash.experiments

import hu.sztaki.ilab.ps.sketch.minhash.MinHashPredict
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._


class MinHashPredictExp

object MinHashPredictExp{

  def main(args: Array[String]): Unit = {

    val modelFile = args(0)
    val wordsInModel = args(1)
    val searchWords = args(2)
    val trainFile = args(3)
    val predictionFile = args(4)
    val delimiter = args(5)
    val workerParallelism = args(6).toInt
    val psParallelism = args(7).toInt
    val iterationWaitTime = args(8).toLong
    val pullLimit = args(9).toInt
    val numHashes = args(10).toInt
    val K = args(11).toInt


    val wordList = scala.io.Source.fromFile(wordsInModel).getLines.toList
    val hashToWord: Map[Int, String] = wordList.map(word => word.hashCode -> word).toMap

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val model: DataStream[(Int, Either[(Int, Array[Long]), Array[Long]])] = env
      .readTextFile(modelFile)
      .map(line => {
        val spl = line.split(":")
        val id = spl(0).toInt
        val params = spl(1)
        val vec: Array[Long] = params.split(",").map(_.toLong)
        (id, Right(vec))
      })

    val words = env.readTextFile(searchWords)

    val train = env
      .readTextFile(trainFile)
      .flatMap(value => {
        val id = value.split(delimiter)(0)
        val tweet = value.split(delimiter)(5).split(" ").filter(wordList.contains)
        if (tweet.nonEmpty)
          Some((id, tweet))
        else None
      })

    MinHashPredict
      .minhashPredict(words, train, model, numHashes, K, workerParallelism, psParallelism, pullLimit, iterationWaitTime)
      .map(value => {
        var s = s"${hashToWord(value._1)} - (${hashToWord(value._2.head._1)},${value._2.head._2})"
        for ((id, score) <- value._2.tail) {
          s += s", (${hashToWord(id)},$score)"
        }
        s
      }).setParallelism(psParallelism)
      .writeAsText(predictionFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1)

    env.execute()
  }
}