package hu.sztaki.ilab.ps.matrix.factorization

case class RandomFactorInitializerDescriptor(numFactors: Int)
  extends FactorInitializerDescriptor {

  override def open(): FactorInitializer =
    new RandomFactorInitializer(scala.util.Random, numFactors)
}
