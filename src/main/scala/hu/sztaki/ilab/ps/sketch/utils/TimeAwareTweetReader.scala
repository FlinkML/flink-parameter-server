package hu.sztaki.ilab.ps.sketch.utils

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

class TimeAwareTweetReader(delimiter: String, searchWords: List[String], timeStamp: Long, windowSize: Int)
  extends RichFlatMapFunction[String, (String, Array[String], Int)]{

  override def flatMap(value: String, out: Collector[(String, Array[String], Int)]): Unit = {
    val id =  value.split(delimiter)(0)
    val tweet = value.split(delimiter)(5).split(" ").map(_.toLowerCase).filter(searchWords.contains(_))
    val timeSlot = ((value.split(delimiter)(1).toLong - timeStamp) / (windowSize * 60 * 60)).toInt
    if(tweet.nonEmpty){
      out.collect((id, tweet, timeSlot))
    }
  }
}
