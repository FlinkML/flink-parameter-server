package hu.sztaki.ilab.ps.matrix.factorization.factors

/**
  * Holds a function to initialize a vector based on its id.
  *
  * E.g. a [[RandomFactorInitializer]] initializes a vector with random numbers.
  */
trait FactorInitializer {
  def nextFactor(id: Int): Array[Double]
}

/**
  * Function that can create a [[FactorInitializer]]
  *
  * This should be used for retrieving non-serializable
  * resources (e.g. random generator) on a cluster machine.
  */
trait FactorInitializerDescriptor {
  // TODO consider replacing this with @transient lazy val
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
