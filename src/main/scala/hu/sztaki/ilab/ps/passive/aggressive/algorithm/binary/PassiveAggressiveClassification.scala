package hu.sztaki.ilab.ps.passive.aggressive.algorithm.binary

import breeze.linalg._

/**
  * Implement the algorithm of the binary passive-aggressive classification from the paper:
  *
  * Crammer et. al.: "Online Passive-Aggressive Algorithms", 2006
  * http://jmlr.csail.mit.edu/papers/volume7/crammer06a/crammer06a.pdf
  */
object PassiveAggressiveClassification {
  def buildPA(): PassiveAggressiveClassification = new PassiveAggressiveClassificationImpl()

  def buildPAI(aggressiveness: Double): PassiveAggressiveClassification = new PassiveAggressiveClassificationImplI(aggressiveness)

  def buildPAII(aggressiveness: Double): PassiveAggressiveClassification = new PassiveAggressiveClassificationImplII(aggressiveness)
}

/**
  *
  * @param aggressiveness set the aggressiveness level of the algorithm. Denoted by C in paper.
  */
abstract class PassiveAggressiveClassification(protected val aggressiveness: Double) extends Serializable {

  /**
    * Calculate the value which is denoted with tau in paper.
    *
    * @param dataPoint denoted by x_t in paper.
    * @param loss suffer loss, denoted with l_t_ in paper
    * @return
    */
  protected def getTau(dataPoint: SparseVector[Double], loss: Double) : Double

  /**
    * Calculate the delta value for the model update.
    *
    * @param dataPoint denoted by x_t in paper.
    * @param model the corresponding model vector for the data. Denoted by w_t in paper.
    *              The active keyset of the model vector should equal to the keyset of the data.
    * @param label the classification label. It should be in set (1.0, -1.0)
    * @return
    */
  def delta(dataPoint: SparseVector[Double], model: SparseVector[Double], label: Int): SparseVector[Double] = {
//    suffer loss, denoted with l_t in paper
    val loss = math.max(0, 1 - label * (dataPoint dot model))
    val multiplier = getTau(dataPoint, loss) * label
    dataPoint *:* multiplier
  }

  /**
    * Predict label based on the actual model
    * @param dataPoint denoted by x_t in paper.
    * @param model the corresponding model vector for the data. Denoted by w_t in paper.
    *              The active keyset of the model vector should equal to the keyset of the data.
    * @return
    */
  def predict(dataPoint: SparseVector[Double], model: Vector[Double]): Int =
    Math.signum(model dot dataPoint).toInt

  /**
    *
    * @param dataPoint denoted by x_t in paper.
    * @param loss denoted by l_t in paper.
    * @param denominatorConst
    * @return
    */
  protected def quotient(dataPoint: SparseVector[Double], loss: Double, denominatorConst: Double): Double = {
    val normSquare = dataPoint dot dataPoint
    if (denominatorConst == 0) loss / normSquare
    else loss / (normSquare + denominatorConst)
  }
}

/**
  * The implementation of the algorithm variation witch is referred by PA in paper.
  */
class PassiveAggressiveClassificationImpl extends PassiveAggressiveClassification(0) {
  override def getTau(data: SparseVector[Double], loss: Double): Double = quotient(data, loss, 0)

}

/**
  * The implementation of the algorithm variation witch is referred by PA-I in paper.
  */
class PassiveAggressiveClassificationImplI(aggressiveness: Double) extends PassiveAggressiveClassification(aggressiveness) {
  override def getTau(data: SparseVector[Double], loss: Double): Double = Math.min(aggressiveness, quotient(data, loss, 0))
}

/**
  * The implementation of the algorithm variation witch is referred by PA-II in paper.
  */
class PassiveAggressiveClassificationImplII(aggressiveness: Double) extends PassiveAggressiveClassification(aggressiveness) {
  override def getTau(data: SparseVector[Double], loss: Double): Double = quotient(data, loss, 1 / (2 * aggressiveness))
}


