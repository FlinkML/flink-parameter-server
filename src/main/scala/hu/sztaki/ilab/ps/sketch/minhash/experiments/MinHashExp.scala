package hu.sztaki.ilab.ps.sketch.minhash.experiments

import hu.sztaki.ilab.ps.sketch.minhash.MinHash
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._

/**
  * Created by lukacsg on 2017.11.30..
  */
class MinHashExp

object MinHashExp {

  def main(args: Array[String]): Unit = {

    val srcFile = args(0)
    val searchWords = args(1)
    val modelFile = args(2)
    val delimiter = args(3)
    val workerParallelism = args(4).toInt
    val psParallelism = args(5).toInt
    val iterationWaitTime = args(6).toLong
    val numHashes = args(7).toInt

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val words = scala.io.Source.fromFile(searchWords).getLines.toList
    val src = env
      .readTextFile(srcFile)
      .flatMap(value => {
        val id = value.split(delimiter)(0)
        val tweet = value.split(delimiter)(5).split(" ").filter(words.contains(_))
        if (tweet.nonEmpty)
          Some((id, tweet))
        else None
      })

    MinHash.minhash(
      src,
      numHashes,
      workerParallelism,
      psParallelism,
      iterationWaitTime
    ).map(value => s"${value._1}:${value._2.mkString(",")}")
      .writeAsText(modelFile, FileSystem.WriteMode.OVERWRITE)

    env.execute()

  }
}
