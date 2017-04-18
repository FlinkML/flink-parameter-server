package hu.sztaki.ilab.ps.utils

import java.io.File

import breeze.linalg.{DenseVector, SparseVector, VectorBuilder}
import com.typesafe.config.ConfigFactory
import hu.sztaki.ilab.ps.passive.aggressive.algorithm.binary.PassiveAggressiveClassification
import org.slf4j.LoggerFactory

import scala.collection.immutable.ListMap
import scala.util.{Failure, Success, Try}


class PassiveAggressiveBinaryModelEvaluation

object PassiveAggressiveBinaryModelEvaluation {

  private val log = LoggerFactory.getLogger(classOf[PassiveAggressiveBinaryModelEvaluation])


  def accuracy(model: DenseVector[Double],
               testLines: Traversable[(SparseVector[Double], Option[Boolean])],
               featureCount: Int,
               pac: PassiveAggressiveClassification): Double = {

    var tt = 0
    var ff = 0
    var tf = 0
    var ft = 0
    var cnt = 0
    testLines.foreach { case (vector, label) => label match {
      case Some(l) =>
        val binaryLabel = if (l) 1 else -1
        if (pac.predict(vector, model) == binaryLabel)
          if (binaryLabel == 1)
            tt += 1
          else
            ff += 1
        else if (binaryLabel == 1)
          tf += 1
        else
          ft += 1
        cnt += 1
      case _ => throw new IllegalStateException("Labels shold not be missing.")
    }
    }
    val percent = ((tt + ff).toDouble / cnt) * 100

    percent
  }


}
