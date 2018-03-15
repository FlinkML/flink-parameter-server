package hu.sztaki.ilab.ps.sketch.utils

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

class TweetReader(delimiter: String, searchWords: List[String]) extends RichFlatMapFunction[String, (String, Array[String])]{
  override def flatMap(value: String, out: Collector[(String, Array[String])]): Unit = {
    val id = value.split(delimiter)(0)
    val tweet = value
      .split(delimiter)(5)
      .split(" ")
      .map(_.toLowerCase)
      .filter(searchWords.contains(_))

    if(tweet.nonEmpty){
      out.collect((id, tweet))
    }
  }
}
