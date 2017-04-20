package hu.sztaki.ilab.ps.matrix.factorization.factors

class SGDUpdater(learningRate: Double) extends FactorUpdater {

  override def delta(rating: Double, user: Array[Double], item: Array[Double]): (Array[Double], Array[Double]) = {
    val e = rating - user.zip(item).map { case (x, y) => x * y }.sum
    val userItem = user.zip(item)

    (item.map(i => learningRate * e * i),
     user.map(u => learningRate * e * u))
  }

}
