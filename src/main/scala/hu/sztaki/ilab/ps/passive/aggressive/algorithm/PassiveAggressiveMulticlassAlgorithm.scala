package hu.sztaki.ilab.ps.passive.aggressive.algorithm

import breeze.linalg.{CSCMatrix, DenseMatrix, SparseVector, Vector}

import scala.collection.mutable.ArrayBuffer

/**
  * Common trait for one-versus-all and cost-based multiclass passive-aggressive classification.
  */
trait PassiveAggressiveMulticlassAlgorithm
  extends PassiveAggressiveAlgorithm[breeze.linalg.Vector[Double], Int, CSCMatrix[Double]] {

  def deltaMtx(data: SparseVector[Double], model: CSCMatrix[Double], label: Int): ArrayBuffer[_ <: (Int, breeze.linalg.Vector[Double])]

  override def predict(data: SparseVector[Double], model: CSCMatrix[Double]): Int

  def predict(data: SparseVector[Double], model: DenseMatrix[Double]): Int

  override def delta(data: SparseVector[Double],
                     model: CSCMatrix[Double],
                     label: Int): Iterable[(Int, Vector[Double])] =
    deltaMtx(data, model, label)

}
