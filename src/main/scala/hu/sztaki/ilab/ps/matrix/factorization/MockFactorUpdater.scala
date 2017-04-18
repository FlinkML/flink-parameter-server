package hu.sztaki.ilab.ps.matrix.factorization

class MockFactorUpdater extends FactorUpdater {
  override def nextFactors(rating: Double,
                           user: Array[Double],
                           item: Array[Double]): (Array[Double], Array[Double]) = {
    (user, item)
  }

  override def delta(rating: Double,
                     user: Array[Double],
                     item: Array[Double]): (Array[Double], Array[Double]) = {
    (user, item)
  }
}
