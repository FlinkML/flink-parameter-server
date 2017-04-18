package hu.sztaki.ilab.ps.matrix.factorization

trait FactorInitializer {
  def nextFactor(id: Int): Array[Double]
}
