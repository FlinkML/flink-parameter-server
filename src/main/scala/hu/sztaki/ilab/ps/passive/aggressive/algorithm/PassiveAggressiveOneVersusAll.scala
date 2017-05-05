package hu.sztaki.ilab.ps.passive.aggressive.algorithm

import breeze.linalg._

import scala.collection.mutable.ArrayBuffer

/**
  * Generalizes binary passive-aggressive classification to multiclass with the one-versus-all (OVA) approach.
  * Implements the algorithm of passive-aggressive classification from the paper:
  *
  * Crammer et. al.: "Online Passive-Aggressive Algorithms", 2006
  * http://jmlr.csail.mit.edu/papers/volume7/crammer06a/crammer06a.pdf
  */
object PassiveAggressiveOneVersusAll {
  def buildPA(): PassiveAggressiveOneVersusAll = new PassiveAggressiveOneVersusAllImpl()

  def buildPAI(aggressiveness: Double): PassiveAggressiveOneVersusAll =
    new PassiveAggressiveOneVersusAllImplI(aggressiveness)

  def buildPAII(aggressiveness: Double): PassiveAggressiveOneVersusAll =
    new PassiveAggressiveOneVersusAllImplII(aggressiveness)
}

/**
  *
  * @param aggressiveness set the aggressiveness level of the algorithm. Denoted with C in paper.
  */
abstract class PassiveAggressiveOneVersusAll(protected val aggressiveness: Double)
  extends PassiveAggressiveMulticlassAlgorithm with Serializable {

  /**
    * Calculate the value which is denoted with tau in paper (generalized to the multi classification case)
    *
    * @param normSquare the square of norm of the given feature vector
    * @param loss suffer loss, denoted with l_t in paper
    * @return
    */
  protected def tau(normSquare: Double, loss: DenseVector[Double]): DenseVector[Double]

  def loss(decisionVector: DenseVector[Double], labelVect: DenseVector[Double]): DenseVector[Double] =
    (decisionVector * labelVect).map(q => Math.max(0, 1 - q))


  /**
    * Calculate the delta value for the model update
    *
    * @param dataPoint denoted by x_t in paper.
    * @param model the corresponding model matrix for the data.
    * @param label the multi classification label. It should be transform to (1, -1) vector for the each binary label.
    * @return
    */
  def deltaMtx(dataPoint: SparseVector[Double],
               model: CSCMatrix[Double],
               label: Int): ArrayBuffer[(Int, DenseVector[Double])] = {
    val labelVector = DenseVector.fill[Double](model.cols)(-1)
    labelVector(label) = 1
    val multiplierVect = tau(dataPoint dot dataPoint, loss((model.t * dataPoint).toDenseVector, labelVector)) * labelVector
    val delta = new ArrayBuffer[(Int, DenseVector[Double])]()
    dataPoint.activeIterator.foreach { case (i, v) =>
      delta += ((i, v *:* multiplierVect ))
    }
    delta
  }

  /**
    * Predict label based on the actual model
    * @param dataPoint denoted by x_t in paper.
    * @param model     the corresponding model matrix for the data, containing multiple binary models
    *                  (denoted by w_t in paper).
    *                  The active rows of the model matrix should equal to the keyset of the data point.
    * @return
    */
  def predict(dataPoint: SparseVector[Double], model: CSCMatrix[Double]): Int =
    argmax((model.t * dataPoint).toDenseVector)

  /**
    * @param dataPoint denoted by x_t in paper.
    * @param model     the corresponding model matrix for the data, containing multiple binary models
    *                  (denoted by w_t in paper).
    * @return
    */
  def predict(dataPoint: SparseVector[Double], model: DenseMatrix[Double]): Int =
    /* This function is overloaded because for the common container class (Matrix[Double]) of the 2 matrix type
     * (CSCMatrix[Double] and DenseMatrix[Double]) the multiplication does not work if the transpose is used on the
     * matrix.
     * model.t * data
     * In this situation the library can not calculate the return type because the lack of some implicit class.
     * TODO Should fix it if the library will support this in future.
     */
    argmax(model.t * dataPoint)

}

/**
  * The implementation of the algorithm variation witch is referred by PA in paper
  */
class PassiveAggressiveOneVersusAllImpl extends PassiveAggressiveOneVersusAll(0) {
  override def tau(normSquare: Double, loss: DenseVector[Double]): DenseVector[Double] = loss.map(_ / normSquare)
}

/**
  * The implementation of the algorithm variation witch is referred by PA-I in paper
  */
class PassiveAggressiveOneVersusAllImplI(aggressiveness: Double)
  extends PassiveAggressiveOneVersusAll(aggressiveness) {
  override def tau(normSquare: Double, loss: DenseVector[Double]): DenseVector[Double] =
    loss.map(q => Math.min(aggressiveness, q / normSquare))
}

/**
  * The implementation of the algorithm variation witch is referred by PA-II in paper
  */
class PassiveAggressiveOneVersusAllImplII(aggressiveness: Double)
  extends PassiveAggressiveOneVersusAll(aggressiveness) {
  override def tau(normSquare: Double, loss: DenseVector[Double]): DenseVector[Double] =
    loss.map(_ / (normSquare +  1 / (2 * aggressiveness)))
}

