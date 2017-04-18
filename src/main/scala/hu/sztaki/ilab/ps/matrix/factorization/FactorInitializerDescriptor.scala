package hu.sztaki.ilab.ps.matrix.factorization

trait FactorInitializerDescriptor {
  def open(): FactorInitializer
}

object FactorInitializerDescriptor {

  def apply(init: Int => Array[Double]): FactorInitializerDescriptor =
    new FactorInitializerDescriptor() {
      override def open(): FactorInitializer = new FactorInitializer {
        override def nextFactor(id: Int): Array[Double] = init(id)
      }
    }
}