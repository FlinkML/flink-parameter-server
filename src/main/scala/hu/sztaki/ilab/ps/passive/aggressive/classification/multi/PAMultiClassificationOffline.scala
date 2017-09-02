package hu.sztaki.ilab.ps.passive.aggressive.classification.multi

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.{Condition, ReentrantLock}

import hu.sztaki.ilab.ps.passive.aggressive.algorithm.RandomModelInitializer
import hu.sztaki.ilab.ps.passive.aggressive.entities.{EOFSign, SparseVector}
import hu.sztaki.ilab.ps.client.receiver.SimpleWorkerReceiver
import hu.sztaki.ilab.ps.client.sender.SimpleWorkerSender
import hu.sztaki.ilab.ps.entities.{PSToWorker, Pull, Push, WorkerToPS}
import hu.sztaki.ilab.ps.passive.aggressive.algorithm.RandomModelInitializer
import hu.sztaki.ilab.ps.passive.aggressive.algorithm.binary.PassiveAggressiveFilter
import hu.sztaki.ilab.ps.server.SimplePSLogic
import hu.sztaki.ilab.ps.server.receiver.SimplePSReceiver
import hu.sztaki.ilab.ps.server.sender.SimplePSSender
import hu.sztaki.ilab.ps.{FlinkParameterServer, ParameterServerClient, WorkerLogic}
import org.apache.flink.api.common.functions.{Partitioner, RichFlatMapFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random


//TODO:
// * source close function run 2x ?

class PAMultiClassificationOffline


object PAMultiClassificationOffline {

  private val log = LoggerFactory.getLogger(classOf[PAMultiClassificationOffline])


  // A labeled date consist a spare vector with an Int label
  type LabeledData = (SparseVector[Double], Int)
  type LabeledOptionData = (SparseVector[Double], Option[Int])
  type Source = Either[LabeledData, LabeledData]

  // fixme This type can be used for outputting data at worker.
  type WOut = Unit

  def paBinaryClassificationOffline(trainingSrcNoEOF: DataStream[Source],
                                    testSrcNoEOF: DataStream[Source],
                                    workerParallelism: Int,
                                    psParallelism: Int,
                                    iterations: Int,
                                    pafType: Int,
                                    pafConst: Int,
                                    pullLimit: Long,
                                    iterationWaitTime: Long): DataStream[Either[WOut, (Int, Double)]] = {

    val readParallelism =
      if (trainingSrcNoEOF.parallelism == testSrcNoEOF.parallelism) {
        trainingSrcNoEOF.parallelism
      } else {
        throw new IllegalArgumentException("Train and test DataStreams should have same the parallelism")
      }

    def eofMarker(leftOrRight: LabeledData => Either[LabeledData, LabeledData]) =
      new RichFlatMapFunction[Source, Source] {

        var collector: Option[Collector[Source]] = None

        override def flatMap(line: Source, out: Collector[Source]): Unit = {
          collector = Some(out)
          out.collect(line)
        }

        override def close(): Unit = {
          collector match {
            case Some(c: Collector[Source]) =>
              for (i <- 0 until readParallelism)
                c.collect(leftOrRight(
                  SparseVector.endOfFile(i, -getRuntimeContext.getIndexOfThisSubtask), Int.MinValue))
            case _ => log.error("Nothing to collect from the source, quite tragic.")
          }
        }
      }

    // Parsing the spare vector file
    val trainingDataSource = trainingSrcNoEOF
      .flatMap(eofMarker(Left(_))).setParallelism(readParallelism)

    val testDataSource = testSrcNoEOF
      .flatMap(eofMarker(Right(_))).setParallelism(readParallelism)

    // TODO The shuffle is implemented manually because of eof markers should be sent to all the workers.
    // Is there a better solution?
    val input = trainingDataSource.union(testDataSource).map(x => x).partitionCustom(new Partitioner[LabeledData] {
      override def partition(data: LabeledData, numPartitions: Int): Int = data match {
        case (eof: EOFSign[Double], Int.MinValue) =>
          eof.workerId
        case (vector: SparseVector[Double], label: Int) =>
          Random.nextInt(numPartitions)
      }

    }, (x: Source) => x match {
      case Right(x: LabeledData) => x
      case Left(x: LabeledData) => x
    })

      .flatMap(new RichFlatMapFunction[Source, LabeledOptionData] {

        var collector: Option[Collector[LabeledOptionData]] = None
        var trainingFinished = false
        var testFinished = false
        var EOFsReceivedTraing = 0
        var EOFsReceivedTest = 0
        val predictElements = new ArrayBuffer[SparseVector[Double]]()

        override def flatMap(in: Source, out: Collector[LabeledOptionData]): Unit = in match {
          case Left((eof: EOFSign[Double], Int.MinValue)) =>
            log.info(s"Received training EOF @${eof.workerId} from ${-eof.minusSourceId}")
            EOFsReceivedTraing += 1
            // Start working when all the threads finished reading.
            if (EOFsReceivedTraing >= readParallelism) {
              // This marks the end of input. We can do the work.
              log.info(s"Number of received prediction element which has not been sent.: ${predictElements.length}")
              trainingFinished = true
              predictElements.foreach(x => out.collect((x, None)))
              predictElements clear
            }
            if (trainingFinished && testFinished) {
              for (i <- 0 until workerParallelism) {
                out.collect(SparseVector.endOfFile(i, -getRuntimeContext.getIndexOfThisSubtask), None)
              }
            }
          case Left((vector: SparseVector[Double], label: Int)) =>
            if (!trainingFinished) {
              out.collect((vector, Some(label)))
            } else {
              throw new IllegalStateException("Should not have started to send the predictication element set while waiting for further " +
                "elements to train.")
            }
          case Right((eof: EOFSign[Double], Int.MinValue)) =>
            log.info(s"Received validation EOF @${eof.workerId} from ${-eof.minusSourceId}")
            EOFsReceivedTest += 1
            // Start working when all the threads finished reading.
            if (EOFsReceivedTest >= readParallelism) {
              // This marks the end of input. We can do the work.
              log.info("End of test elements")
              testFinished = true
            }
            if (trainingFinished && testFinished) {
              for (i <- 0 until workerParallelism) {
                out.collect(SparseVector.endOfFile(i, -getRuntimeContext.getIndexOfThisSubtask), None)
              }
            }
          case Right((vector: SparseVector[Double], label: Int)) =>
            if (testFinished) {
              throw new IllegalStateException("Should not have closed the predictication element set while waiting for further " +
                "elements to test.")
            }
            if (trainingFinished) {
              out.collect((vector, None))
            } else {
              predictElements += vector
            }
        }
      }).setParallelism(readParallelism)
      //      .shuffle
      //        The shuffle was implemented by hand because of the eof element concepts that is it should send all the worker. Is there a better solution?
      .partitionCustom(new Partitioner[LabeledOptionData] {
      override def partition(data: LabeledOptionData, numPartitions: Int): Int = data match {
        case (eof: EOFSign[Double], None) =>
          eof.workerId
        case (vector: SparseVector[Double], label: Option[Int]) =>
          Random.nextInt(numPartitions)
      }
    }, x => x)

    val serverLogic = new SimplePSLogic[Double](
      x => RandomModelInitializer.init(), _ + _)

    val paFilter = pafType match {
      case 0 => PassiveAggressiveFilter.buildPAF()
      case 1 => PassiveAggressiveFilter.buildPAFI(pafConst)
      case 2 => PassiveAggressiveFilter.buildPAFII(pafConst)
      case _ => throw new IllegalArgumentException("PassiveAggressiveFilter type can be only in the set (0, 1, 2)")
    }

    val workerLogic = new WorkerLogic[LabeledOptionData, Double, WOut] {

      val trainingData = ArrayBuffer[LabeledData]()
      val testData = ArrayBuffer[SparseVector[Double]]()
      val predictions = mutable.HashMap[SparseVector[Double], Int]()
      val waitingQueue = new mutable.HashMap[Long, mutable.Queue[(Int, mutable.HashMap[Long, Double])]]()

      val paf = paFilter

      // todo gracefully stop thread at end of computation (override close method)
      var workerThread: Thread = null

      // We need to check if all threads finished already
      val EOFsReceived = new AtomicInteger(0)
      @volatile var predictionIsStarted = false

      var pullCounter = 0
      val psLock = new ReentrantLock()
      val canPull: Condition = psLock.newCondition()

      override def onRecv(data: LabeledOptionData,
                          ps: ParameterServerClient[Double, WOut]): Unit = {

        data match {
          case (eof: EOFSign[Double], None) =>
            log.info(s"Received EOF @${eof.workerId} from ${-eof.minusSourceId}")
            EOFsReceived.getAndIncrement()
            // Start working when all the threads finished reading.
            if (EOFsReceived.get() >= readParallelism) {
              // This marks the end of input. We can do the work.
              log.info(s"Number of received trainings data: ${trainingData.length}")
              log.info(s"Number of received test data: ${testData.size}")

              // We start a new Thread to avoid blocking the answers to pulls.
              // Otherwise the work would only start when all the pulls are sent (for all iterations).
              workerThread = new Thread(new Runnable {
                override def run(): Unit = {
                  log.debug("worker thread started")
                  for (iter <- 1 to iterations) {
                    Random.shuffle(trainingData)
                    for (i <- 0 until trainingData.size) {
                      val (vector, label) = trainingData(i)
                      // we assume that the PS client is not thread safe, so we need to sync when we use it.
                      psLock.lock()
                      try {
                        while (pullCounter >= pullLimit) {
                          canPull.await()
                        }
                        val keys = vector.getIndexes
                        pullCounter += keys.size
                        log.debug(s"pull inc: $pullCounter")
                        val waitingValues = new mutable.HashMap[Long, Double]()
                        for (k <- keys) {
                          waitingValues += ((k, Double.NaN))
                          // TODO: The keys can not be long?
                          ps.pull(k.toInt)
                          waitingQueue.getOrElseUpdate(k, mutable.Queue[(Int, mutable.HashMap[Long, Double])]())
                            .enqueue((i, waitingValues))
                        }

                      } finally {
                        psLock.unlock()
                      }

                    }
                  }
                  log.debug("traing pulls finished")

                  psLock.lock()
                  try {
                    while (!waitingQueue.isEmpty) {
                      log.debug(s"WAIT pull inc: ${waitingQueue.size}")
                      canPull.await()
                    }
                  } finally {
                    psLock.unlock()
                  }

                  predictionIsStarted = true

                  assert(waitingQueue.isEmpty)
                  log.debug("predictions started")
                  for (i <- 0 until testData.size) {
                    val vector = testData(i)
                    // we assume that the PS client is not thread safe, so we need to sync when we use it.
                    psLock.lock()
                    try {
                      while (pullCounter >= pullLimit) {
                        canPull.await()
                      }
                      val keys = vector.getIndexes
                      pullCounter += keys.size
                      log.debug(s"pull inc: $pullCounter")
                      val waitingValues = new mutable.HashMap[Long, Double]()
                      for (k <- keys) {
                        waitingValues += ((k, Double.NaN))
                        // TODO: The keys can not be long?
                        ps.pull(k.toInt)
                        waitingQueue.getOrElseUpdate(k, mutable.Queue[(Int, mutable.HashMap[Long, Double])]())
                          .enqueue((i, waitingValues))
                      }

                    } finally {
                      psLock.unlock()
                    }

                  }
                  log.debug("test pulls finished")
                }
              })
              workerThread.start()
            }
          case ((data: SparseVector[Double], Some(label))) =>
            // Since the EOF signals likely won't arrive at the same time, an extra check for the finished sources is needed.
            // todo maybe use assert instead?
            if (workerThread != null) {
              throw new IllegalStateException("Should not have started worker thread while waiting for further " +
                "elements.")
            }
            trainingData += ((data, label))
          case ((data: SparseVector[Double], None)) =>
            // Since the EOF signals likely won't arrive at the same time, an extra check for the finished sources is needed.
            // todo maybe use assert instead?
            if (workerThread != null) {
              throw new IllegalStateException("Should not have started worker thread while waiting for further " +
                "elements.")
            }
            testData += data
        }
      }

      override def onPullRecv(modelId: Int,
                              modelValue: Double,
                              ps: ParameterServerClient[Double, WOut]): Unit = {
        // we assume that the PS client is not thread safe, so we need to sync when we use it.
        psLock.lock()
        try {
          val q = waitingQueue(modelId)
          val (dataId, waitingValues) = waitingQueue(modelId).dequeue()
          assert(waitingValues(modelId).isNaN)
          waitingValues(modelId) = modelValue
          if (waitingValues.values.count(_.isNaN) == 0) {
            if (!predictionIsStarted) {
              val (vector, label) = trainingData(dataId)
              // todo: maybe create an immutable HashMap from mutable HashMap is wasting
              paf.delta(vector, HashMap(waitingValues.toSeq: _*), label)
                .foreach {
                  case (i, v) => ps.push(i.toInt, v)
                }
            } else {
              val vector = testData(dataId)
              // todo: maybe create an immutable HashMap from mutable HashMap is wasting
              predictions(vector) = paf.predict(vector, HashMap(waitingValues.toSeq: _*))
            }
          }
          //          because the predactions the modelId must remove
          if (q.isEmpty) waitingQueue.remove(modelId)

          pullCounter -= 1
          canPull.signal()
          log.debug(s"pull dec: $pullCounter")
        } finally {
          psLock.unlock()
        }
      }

      override def close(): Unit = {
        predictions.foreach {
          case (vector: SparseVector[Double], label: Int) =>
            log.info(s"###PS###t;${label};[${vector.getValues mkString ","}]")
        }
      }
    }


    val paramPartitioner: WorkerToPS[Double] => Int = {
      case WorkerToPS(partitionId, msg) => msg match {
        case Left(Pull(paramId)) => Math.abs(paramId) % psParallelism
        case Right(Push(paramId, delta)) => Math.abs(paramId) % psParallelism
      }
    }

    val wInPartition: PSToWorker[Double] => Int = {
      case PSToWorker(workerPartitionIndex, _) => workerPartitionIndex
    }


    val modelUpdates = FlinkParameterServer.transform(input, workerLogic, serverLogic,
      paramPartitioner = paramPartitioner,
      wInPartition = wInPartition,
      workerParallelism,
      psParallelism,
      new SimpleWorkerReceiver[Double](),
      new SimpleWorkerSender[Double](),
      new SimplePSReceiver[Double](),
      new SimplePSSender[Double](),
      iterationWaitTime)
//      .setParallelism(psParallelism)

    modelUpdates
  }

}
