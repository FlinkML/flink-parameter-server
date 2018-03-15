package hu.sztaki.ilab.ps.sketch.tug.of.war

import hu.sztaki.ilab.ps.sketch.tug.of.war.pslogic.TimeAwareToWPSLogic
import hu.sztaki.ilab.ps.{FlinkParameterServer, ParameterServerClient, WorkerLogic}
import hu.sztaki.ilab.ps.sketch.utils.Utils.Vector
import net.openhft.hashing.LongHashFunction
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

class TimeAwareTugOfWar {

}

object TimeAwareTugOfWar {

  def tugOfWar(src: DataStream[(String, Array[String], Int)],
               numHashes: Int,
               workerParallelism: Int,
               psParallelism: Int,
               iterationWaitTime: Long) : DataStream[((Int, Int), Vector)] = {

    val workerLogic = new WorkerLogic[(String, Array[String], Int), (Int, Array[Long]), Any] {


      override def onRecv(data: (String, Array[String], Int), ps: ParameterServerClient[(Int, Array[Long]), Any]): Unit = {
        val id = data._1.toLong
        val tweet = data._2

        val hashArray = (for (i <- 0 to math.ceil(numHashes / 64).toInt) yield LongHashFunction.xx(i).hashLong(id)).toArray


        for(word <- tweet) {
          ps.push(word.hashCode, (data._3, hashArray))
        }
      }

      override def onPullRecv(paramId: Int, paramValue: (Int, Array[Long]), ps: ParameterServerClient[(Int, Array[Long]), Any]): Unit = ???
    }

    val serverLogic = new TimeAwareToWPSLogic(numHashes)

    val modelUpdates = FlinkParameterServer.transform(
      src,
      workerLogic,
      serverLogic,
      workerParallelism,
      psParallelism,
      iterationWaitTime)

    modelUpdates
      .flatMap(new RichFlatMapFunction[Either[Any, ((Int, Int), Vector)], ((Int, Int), Vector)] {
        override def flatMap(value: Either[Any,  ((Int, Int), Vector)], out: Collector[ ((Int, Int), Vector)]): Unit = {
          value match {
            case Left(_) =>
            case Right(param) => out.collect(param)
          }
        }
      })
  }
}
