package hu.sztaki.ilab.ps.matrix.factorization.factors

import scala.util.Random

class RangedRandomFactorInitializer(random: Random, numFactors: Int, rangeMin: Double, rangeMax: Double)
  extends FactorInitializer{
  override def nextFactor(id: Int): Array[Double] = {
    Array.fill(numFactors)(rangeMin + (rangeMax - rangeMin) * random.nextDouble)
  }
}

case class RangedRandomFactorInitializerDescriptor(numFactors: Int, rangeMin: Double, rangeMax: Double)
  extends FactorInitializerDescriptor{

  override def open(): FactorInitializer =
    new RangedRandomFactorInitializer(scala.util.Random, numFactors, rangeMin, rangeMax)

}
