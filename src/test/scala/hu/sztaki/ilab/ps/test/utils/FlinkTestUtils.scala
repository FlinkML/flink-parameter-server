package hu.sztaki.ilab.ps.test.utils

import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FlinkTestUtils {

  case class SuccessException[T](content: T) extends Exception {
    override def toString: String = s"SuccessException($content)"
  }

  case class NoSuccessExceptionReceived() extends Exception

  def executeWithSuccessCheck[T](env: StreamExecutionEnvironment)(checker: T => Unit): Unit = {
    try {
      env.execute()
      throw NoSuccessExceptionReceived()
    } catch {
      case e: JobExecutionException =>
        val rootCause = Stream.iterate[Throwable](e)(_.getCause()).takeWhile(_ != null).last
        rootCause match {
          case successException: SuccessException[T] =>
            checker(successException.content)
          case otherCause =>
            throw e
        }
      case e: Throwable => throw e
    }
  }
}
