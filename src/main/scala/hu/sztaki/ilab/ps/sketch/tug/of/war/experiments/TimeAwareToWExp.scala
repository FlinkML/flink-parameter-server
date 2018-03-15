package hu.sztaki.ilab.ps.sketch.tug.of.war.experiments

import hu.sztaki.ilab.ps.sketch.tug.of.war.TimeAwareTugOfWar
import hu.sztaki.ilab.ps.sketch.utils.TimeAwareTweetReader
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._

class TimeAwareToWExp {

}

object TimeAwareToWExp {

  def main(args: Array[String]): Unit = {
    val srcFile = args(0)
    val searchWordsFile = args(1)
    val delimiter = args(2)
    val modelFile = args(3)
    val workerParallelism = args(4).toInt
    val psParallelism = args(5).toInt
    val iterationWaitTime = args(6).toLong
    val numHashes = args(7).toInt
    val timeStamp = args(8).toLong
    val windowSize = args(9).toInt

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val searchWords = scala.io.Source.fromFile(searchWordsFile).getLines.toList

    val src = env
      .readTextFile(srcFile)
      .flatMap(new TimeAwareTweetReader(delimiter, searchWords, timeStamp, windowSize))

    TimeAwareTugOfWar.tugOfWar(
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
