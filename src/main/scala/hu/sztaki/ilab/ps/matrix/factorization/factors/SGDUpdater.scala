package hu.sztaki.ilab.ps.matrix.factorization.factors

import breeze.numerics.sigmoid

class SGDUpdater(learningRate: Double) extends FactorUpdater {

  override def delta(rating: Double, user: Array[Double], item: Array[Double]): (Array[Double], Array[Double]) = {
    val error = sigmoid(rating - user.zip(item).map { case (x, y) => x * y }.sum)

    (item.map(i => learningRate * error * i),
      user.map(u => learningRate * error * u))
  }

}
