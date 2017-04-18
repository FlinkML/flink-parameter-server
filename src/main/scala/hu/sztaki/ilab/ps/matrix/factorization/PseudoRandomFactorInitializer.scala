package hu.sztaki.ilab.ps.matrix.factorization

import scala.util.Random

class PseudoRandomFactorInitializer(numFactors: Int)
  extends FactorInitializer {
  override def nextFactor(id: Int): Array[Double] = {
    val random = new Random(id)
    Array.fill(numFactors)(random.nextDouble)
  }
}
