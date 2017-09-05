package hu.sztaki.ilab.ps.matrix.factorization.sink

import java.io.{FileWriter, PrintWriter}

import hu.sztaki.ilab.ps.matrix.factorization.utils.Utils.{ItemId, UserId}
import hu.sztaki.ilab.ps.matrix.factorization.utils.Vector.VectorLength
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable

object nDCGSink {

  /**
    * Writes result sum and average nDCG and hit to a human-readable text file.
    * If periodLength is given, results per period are also printed
    * File format:
    *
    * Number of invokes: #<br>
    * Sum nDCG: #<br>
    * Avg nDCG: #<br>
    * Hit: #<br>
    * <br>
    * <br>
    * Period: #<br>
    *     :Number of Invokes: #<br>
    *     :Sum nDCG: #<br>
    *     :Avg nDCG: #<br>
    *     :Hit: #<br>
    * <br>
    * (lines after Period are indented by a tab)
    *
    * @param	topK
    * A flink data stream carrying the user ID, item ID, and timestamp of the rating that queried the TopK,
    * and a List containing the TopK predicted ratings and item IDs for that user sorted in descending order
    * @param	fileName
    * The name of the output file
    * @param periodLength
    * The length of periods. In case 0 is specified, no periods will be printed
    */
  def nDCGToFile(topK: DataStream[(UserId, ItemId, Long, List[(Double, ItemId)])],
                 fileName: String,
                 periodLength: Int = 0)(implicit d: DummyImplicit): DataStreamSink[(ItemId, Long, List[(VectorLength, ItemId)])] = {
    topK
      .map( _ match { case (_, item, timestamp, tk)  => (item, timestamp, tk) })
      .addSink(new nDCGSink(fileName, periodLength)).setParallelism(1)
  }

  /**
    * Writes result sum and average nDCG and hit to a human-readable text file.
    * If periodLength is given, results per period are also printed
    * File format:
    *
    * Number of invokes: #<br>
    * Sum nDCG: #<br>
    * Avg nDCG: #<br>
    * Hit: #<br>
    * <br>
    * <br>
    * Period: #<br>
    *     :Number of Invokes: #<br>
    *     :Sum nDCG: #<br>
    *     :Avg nDCG: #<br>
    *     :Hit: #<br>
    * <br>
    * (lines after Period are indented by a tab)
    *
    * @param	topK
    * A flink data stream carrying the item ID and timestamp of the rating that queried the TopK,
    * and a List containing the TopK predicted ratings and item IDs for that user sorted in descending order
    * @param	fileName
    * The name of the output file
    * @param periodLength
    * The length of periods. In case 0 is specified, no periods will be printed
    */
  def nDCGToFile(topK: DataStream[(ItemId, Long, List[(Double, ItemId)])], fileName: String, periodLength: Int) {
    topK
      .addSink(new nDCGSink(fileName, periodLength)).setParallelism(1)
  }

  /**
    * Appends a CSV file with one line containing the number of invokes,
    * the average nDCG, and the rate of hits, separated by commas
    *
    * @param	topK
    * A flink data stream carrying the user ID, item ID, and timestamp of the rating that queried the TopK,
    * and a List containing the TopK predicted ratings and item IDs for that user sorted in descending order
    * @param	fileName
    * The name of the output file
    */
  def nDCGToCsv(topK: DataStream[(UserId, ItemId, Long, List[(Double, ItemId)])], fileName: String)(implicit d: DummyImplicit) {
    topK
      .map(_ match { case (_, item, timestamp, tk)  => (item, timestamp, tk) })
      .addSink(new nDCGSink(fileName, 0, true, true))
  }

  /**
    * Appends a CSV file with one line containing the number of invokes,
    * the average nDCG, and the rate of hits, separated by commas
    *
    * @param	topK
    * A flink data stream carrying the item ID and timestamp of the rating that queried the TopK,
    * and a List containing the TopK predicted ratings and item IDs for that user sorted in descending order
    * @param	fileName
    * The name of the output file
    */
  def nDCGToCsv(topK: DataStream[(ItemId, Long, List[(Double, ItemId)])], fileName: String) {
    topK
      .addSink(new nDCGSink(fileName, 0, true, true))
  }

  /**
    * Writes a CSV file with one line per period containing the number of invokes,
    * the average nDCG, and the rate of hits, separated by commas
    *
    * @param	topK
    * A flink data stream carrying the user ID, item ID, and timestamp of the rating that queried the TopK,
    * and a List containing the TopK predicted ratings and item IDs for that user sorted in descending order
    * @param	fileName
    * The name of the output file
    * @param periodLength
    * The length of periods. In case 0 is specified, no periods will be printed
    * @param append
    * Whether to append the output file. Default value is false.
    */
  def nDCGPeriodsToCsv(topK: DataStream[(UserId, ItemId, Long, List[(Double, ItemId)])],
                       fileName: String, periodLength: Int, append: Boolean = false)(implicit d: DummyImplicit) {
    topK
      .map(_ match { case (_, item, timestamp, tk)  => (item, timestamp, tk) })
      .addSink(new nDCGSink(fileName, periodLength, append, true))
  }

  /**
    * Writes a CSV file with one line per period containing the number of invokes,
    * the average nDCG, and the rate of hits, separated by commas
    *
    * @param	topK
    * A flink data stream carrying the user ID, item ID, and timestamp of the rating that queried the TopK,
    * and a List containing the TopK predicted ratings and item IDs for that user sorted in descending order
    * @param	fileName
    * The name of the output file
    * @param periodLength
    * The length of periods. In case 0 is specified, no periods will be printed
    * @param append
    * Whether to append the output file. Default value is false.
    */
  def nDCGPeriodsToCsv(topK: DataStream[(ItemId, Long, List[(Double, ItemId)])],
                       fileName: String, periodLength: Int, append: Boolean) {
    topK
      .addSink(new nDCGSink(fileName, periodLength, append, true))
  }
}

/**
  * Evaluates nDCG from TopK output of a recommender system, and writes it to a human-readable text file,
  * or a CSV file that can be used for plotting. Output format of text file:
  *
  * Number of invokes: #<br>
  * Sum nDCG: #<br>
  * Avg nDCG: #<br>
  * Hit: #<br>
  * <br>
  * <br>
  * Period: #<br>
  *     :Number of Invokes: #<br>
  *     :Sum nDCG: #<br>
  *     :Avg nDCG: #<br>
  *     :Hit: #<br>
  * <br>
  * (lines after Period are indented by a tab)
  * In case a non-zero period is specified, there will be a period block for each period.
  *
  * Output format of CSV file in case a period of 0 is specified:
  *
  * invokes,averagenDCG,hitrate
  *
  * in case a non-zero period is specified:
  *
  * periodNumber,invokes,averagenDCG,hitrate
  *
  * @param	fileName
  * Name of the output file
  * @param periodLength
  * Length of periods
  * @param append
  * Whether to append the file
  * @param csv
  * Whether to use CSV
  *
  */
class nDCGSink(fileName: String, periodLength: Int = 86400, append: Boolean = false, csv: Boolean = false)
  extends RichSinkFunction[(ItemId, Long, List[(Double, ItemId)])]{


  var sumnDCG = 0.0
  var counter = 0
  var hit = 0

  val log2: VectorLength = Math.log(2)

  val perDay = new mutable.HashMap[Int, (Int, Double, Int)]

  override def invoke(value: (ItemId, Long, List[(Double, ItemId)])): Unit = {
    val timestamp = value._2

    val index = value._3.indexWhere (
      recommendation => recommendation._2 == value._1 ) match {

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

    if (periodLength > 0) {
      val (todayInv, todaySum, todayHit) = perDay.getOrElse((timestamp / periodLength).toInt, (0, 0.0, 0))
      perDay.update((timestamp / periodLength).toInt, (todayInv + 1, todaySum + nDCG, todayHit + (if (nDCG != 0.0) 1 else 0)))
    }
  }

  override def close(): Unit = {
    val outputFile = new PrintWriter(new FileWriter(fileName, append))

    val avgnDCG = sumnDCG / counter

    if (csv) {
      if (periodLength <= 0) {
        val hitRate = hit.toDouble / counter
        outputFile write s"$counter,$avgnDCG,$hitRate\n"
      }
    }
    else {
      outputFile write s"Number of invokes: $counter\n"
      outputFile write s"Sum nDCG: $sumnDCG\n"
      outputFile write s"Avg nDCG: $avgnDCG\n"
      outputFile write s"Hit: $hit\n\n"
    }

    if (periodLength > 0) {
      for ((day, (inv, sum, hit)) <- perDay.toArray.sortBy(_._1)) {
        val avg = sum / inv
        if (csv) {
          val hitrate = hit.toDouble / inv
          outputFile write s"$day,$inv,$avg,$hitrate\n"
        }
        else {
          outputFile write s"Period $day\n"
          outputFile write s"\t:Number of invokes: $inv\n"
          outputFile write s"\t:Sum nDCG: $sum\n"
          outputFile write s"\t:Avg nDCG: $avg\n"
          outputFile write s"\t:Hit: $hit\n\n"
        }
      }
    }

    outputFile.close()

  }
}
