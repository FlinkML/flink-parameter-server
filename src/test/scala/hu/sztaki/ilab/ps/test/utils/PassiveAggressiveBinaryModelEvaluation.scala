package hu.sztaki.ilab.ps.test.utils

import breeze.linalg.{DenseVector, SparseVector}
import hu.sztaki.ilab.ps.passive.aggressive.algorithm.PassiveAggressiveBinaryAlgorithm
import org.slf4j.LoggerFactory

class PassiveAggressiveBinaryModelEvaluation

object PassiveAggressiveBinaryModelEvaluation {

  private val log = LoggerFactory.getLogger(classOf[PassiveAggressiveBinaryModelEvaluation])


  def accuracy(model: DenseVector[Double],
               testLines: Traversable[(SparseVector[Double], Option[Boolean])],
               featureCount: Int,
               pac: PassiveAggressiveBinaryAlgorithm): Double = {

    var tt = 0
    var ff = 0
    var tf = 0
    var ft = 0
    var cnt = 0
    testLines.foreach { case (vector, label) => label match {
      case Some(lab) =>
        val real = lab
        val predicted = pac.predict(vector, model)
        (real, predicted) match {
          case (true, true) => tt +=1
          case (false, false) => ff +=1
          case (true, false) => tf +=1
          case (false, true) => ft +=1
        }
        cnt += 1
      case _ => throw new IllegalStateException("Labels shold not be missing.")
    }
    }
    val percent = ((tt + ff).toDouble / cnt) * 100

    percent
  }


}
