package hu.sztaki.ilab.ps.matrix.factorization

import scala.util.Random

class RandomFactorInitializer(random: Random, numFactors: Int)
  extends FactorInitializer {
  override def nextFactor(id: Int): Array[Double] = {
    Array.fill(numFactors)(random.nextDouble)
  }
}
