package hu.sztaki.ilab.ps.sketch.bloom.filter.experiments

import hu.sztaki.ilab.ps.sketch.bloom.filter.TimeAwareBloomFilter
import hu.sztaki.ilab.ps.sketch.utils.TimeAwareTweetReader
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._

/**
  * Basic experiment for the Time Aware Bloom filter, where we read in a list of chosen words and build the filters for them based on the incoming tweets
  * Tweets must have the given format specified in TweetReader
  */
object TimeAwareBloomFilterExp {

  def main(args: Array[String]): Unit = {

    val srcFile = args(0)
    val searchWords = args(1)
    val delimiter = args(2)
    val modelFile = args(3)
    val workerParallelism = args(4).toInt
    val psParallelism = args(5).toInt
    val iterationWaitTime = args(6).toLong
    val numHashes = args(7).toInt
    val arraySize = args(8).toInt
    val timeStamp = args(9).toLong
    val windowSize = args(10).toInt

    println(args.toList)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val words = scala.io.Source.fromFile(searchWords).getLines.toList

    val src = env
      .readTextFile(srcFile)
      .flatMap(new TimeAwareTweetReader(delimiter, words, timeStamp, windowSize))

    TimeAwareBloomFilter.bloomFilter(
      src,
      numHashes,
      arraySize,
      workerParallelism,
      psParallelism,
      iterationWaitTime)
      .map(value => s"${value._1}:${value._2.mkString(",")}")
      .writeAsText(modelFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1)

    env.execute()
  }
}
