package hu.sztaki.ilab.ps.passive.aggressive.algorithm

import breeze.linalg._

import scala.collection.mutable.ArrayBuffer

/**
  * Implements the cost-based multiclass passive-aggressive classification from the paper:
  *
  * Crammer et. al.: "Online Passive-Aggressive Algorithms", 2006
  * http://jmlr.csail.mit.edu/papers/volume7/crammer06a/crammer06a.pdf
  */
object PassiveAggressiveCostBased {

  /**
    * Prediction-based multiclass classification. Denoted by PB in paper.
    *
    * @param cost Denoted with ρ(Rho) in paper. Specifically, for every pair of labels (y, y′) there is a cost ρ(y, y′) associated with predicting y′
    *             when the correct label is y.
    * @return
    */
  def buildPB(cost: (Int, Int) => Double): PassiveAggressiveCostBased = new PassiveAggressiveCostBasedImplPB(cost)

  /**
    * Max-loss multiclass classification. Denoted by ML in paper.
    *
    * @param cost Denoted with ρ(Rho) in paper. Specifically, for every pair of labels (y, y′) there is a cost ρ(y, y′)
    *             associated with predicting y′ when the correct label is y.
    * @return
    */
  def buildML(cost: (Int, Int) => Double): PassiveAggressiveCostBased =
    new PassiveAggressiveCostBasedImplML(cost)
}

/**
  *
  * @param cost Denoted with ρ(Rho) in paper. Specifically, for every pair of labels (y, y′) there is a cost ρ(y, y′)
  *             associated with predicting y′ when the correct label is y.
  */
abstract class PassiveAggressiveCostBased(protected val cost: (Int, Int) => Double)
  extends PassiveAggressiveMulticlassAlgorithm with Serializable {

  /**
    * Suffer loss, denoted with l_t in paper
    *
    * @param decisionVector partial result to make the calculation easier.
    * @param q              the label which the loss is calculated. It depends on the type of algorithm.
    * @param label          the correct label.
    * @return the amount of the loss.
    */
  def loss(decisionVector: DenseVector[Double], q: Int, label: Int): Double =
    decisionVector(q) - decisionVector(label) + Math.sqrt(cost(label, q))

  /**
    * Calculate the value which is denoted with tau in paper
    *
    * @param dataPoint partial result to make the calculation easier.
    * @param loss      suffer loss, denoted with l_t in paper.
    * @return
    */
  def tau(dataPoint: SparseVector[Double], loss: Double): Double = loss / (2 * (dataPoint dot dataPoint))

  /**
    * Calculate the delta value for the model update
    *
    * @param dataPoint
    * @param model the corresponding model vector for the data.
    *              The active keyset of the model vector should equal to the keyset of the data
    * @param label the classification label. It shold be in set (1, -1)
    * @return
    */
  def deltaMtx(dataPoint: SparseVector[Double],
               model: CSCMatrix[Double],
               label: Int): ArrayBuffer[(Int, SparseVector[Double])] = {
    val labelCount = model.cols
    val decisionVector = (model.t * dataPoint).toDenseVector
    val q = quotient(decisionVector, label)
    val t = tau(dataPoint, loss(decisionVector, q, label))
    val delta = new ArrayBuffer[(Int, SparseVector[Double])]()
    if (q != label) {
      dataPoint.activeIterator.foreach { case (i, v) =>
        val deltaVector = SparseVector.zeros[Double](labelCount)
        deltaVector(label) = t * v
        deltaVector(q) = -t * v
        delta += ((i, deltaVector))
      }
    }
    delta
  }

  /**
    * Predict label based on the actual model
    *
    * @param dataPoint
    * @param model
    * @return
    */
  def predict(dataPoint: SparseVector[Double], model: CSCMatrix[Double]): Int =
    argmax((model.t * dataPoint).toDenseVector)

  /**
    * @param dataPoint
    * @param model
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

  /**
    * Denote with q_t in paper.
    *
    * @param decisionVector is a partial results to make the calculation easier.
    * @return the label which is depended on the type of algorithm
    */
  protected def quotient(decisionVector: DenseVector[Double], label: Int): Int
}

/**
  * Algorithm variation which is referred by PB in paper.
  */
class PassiveAggressiveCostBasedImplPB(cost: (Int, Int) => Double) extends PassiveAggressiveCostBased(cost) {
  override def quotient(decisionVector: DenseVector[Double], label: Int): Int =
    argmax(decisionVector)
}

/**
  * Algorithm variation which is referred by ML in paper.
  */
class PassiveAggressiveCostBasedImplML(cost: (Int, Int) => Double) extends PassiveAggressiveCostBased(cost) {
  override def quotient(decisionVector: DenseVector[Double], label: Int): Int =
    argmax(decisionVector.mapPairs((i, v) => v - decisionVector(label) + Math.sqrt(cost(label, i))))
}


