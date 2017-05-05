package hu.sztaki.ilab.ps.test.utils

import breeze.linalg.{DenseMatrix, SparseVector}
import hu.sztaki.ilab.ps.passive.aggressive.algorithm.PassiveAggressiveMulticlassAlgorithm
import org.slf4j.LoggerFactory

class PassiveAggressiveMultiModelEvaluation

object PassiveAggressiveMultiModelEvaluation {

  private val log = LoggerFactory.getLogger(classOf[PassiveAggressiveMultiModelEvaluation])

  def accuracy(model: DenseMatrix[Double], testLines: Traversable[(SparseVector[Double], Option[Int])],
               featureCount: Int, pac: PassiveAggressiveMulticlassAlgorithm): Double = {

    var hit = 0
    var cnt = 0
    testLines.foreach{case(vector, label) => label match {
      case Some(l) =>
      if (pac.predict(vector, model) == l) hit += 1
      cnt += 1
      case _ => throw new IllegalStateException("Labels should not be missing.")
    }}
    val percent = (hit.toDouble / cnt) * 100
    percent
  }

}
