package hu.sztaki.ilab.ps.matrix.factorization.factors

import scala.util.Random

class RandomFactorInitializer(random: Random, numFactors: Int)
  extends FactorInitializer {
  override def nextFactor(id: Int): Array[Double] = {
    Array.fill(numFactors)(random.nextDouble)
  }
}

case class RandomFactorInitializerDescriptor(numFactors: Int)
  extends FactorInitializerDescriptor {

  override def open(): FactorInitializer =
    new RandomFactorInitializer(scala.util.Random, numFactors)
}