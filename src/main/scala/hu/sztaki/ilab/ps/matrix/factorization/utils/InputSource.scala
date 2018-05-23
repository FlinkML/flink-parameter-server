package hu.sztaki.ilab.ps.matrix.factorization.utils

import java.util.Calendar

import hu.sztaki.ilab.ps.matrix.factorization.utils.InputTypes.{AnyOrWatermark, EventWithTimestamp}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.watermark.Watermark

import scala.io.{BufferedSource, Source}

class InputSource[T <: EventWithTimestamp](dataFilePath: String, servingSpeed: Int,
                                           fromString: String => T, baseDataStartTime: Option[Long], simulationEndTime: Option[Long])
  extends SourceFunction[T] {

  private val simEndTime: Long = simulationEndTime.getOrElse(0L)
  override def cancel(): Unit = ???

  def createList(reader: BufferedSource): List[T] = {
    reader
      .getLines()
      .map(fromString)
      .toList
  }

  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
    val reader = Source.fromFile(dataFilePath)

    val events: List[T] = createList(reader)

    val dataStartTime: Long =
      baseDataStartTime match {
        case Some(dst) => dst
        case None => events.head.getEventTime
      }

    val sortedEvents = events
      .map(x => AnyOrWatermark(x.getEventTime, x))
      .toArray

    val servingStartTime = Calendar.getInstance.getTimeInMillis

    sortedEvents
      .foreach( event => {
        val now = Calendar.getInstance().getTimeInMillis
        val servingTime = toServingTime(servingStartTime, dataStartTime, event.time)
        val waitTime = servingTime - now

        Thread.sleep(math.max(waitTime, 0))

        event.value match {
          case v: T => ctx.collectWithTimestamp(v, event.time)
          case wm: Watermark => ctx.emitWatermark(wm)
        }
      })
  }

  private def toServingTime(servingStartTime: Long, dataStartTime: Long, eventTime: Long) = {

    if(simEndTime != 0 && eventTime >= simEndTime){
      val dataDiff = eventTime - simEndTime
      ((servingStartTime + (simEndTime / servingSpeed)) + dataDiff) - (dataStartTime / servingSpeed)
    }
    else{
      val dataDiff = eventTime - dataStartTime
      servingStartTime + (dataDiff / servingSpeed)
    }
  }
}
