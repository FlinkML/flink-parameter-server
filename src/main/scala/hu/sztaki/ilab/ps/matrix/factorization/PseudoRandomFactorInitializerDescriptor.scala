package hu.sztaki.ilab.ps.matrix.factorization

case class PseudoRandomFactorInitializerDescriptor(numFactors: Int)
  extends FactorInitializerDescriptor {

  override def open(): FactorInitializer =
    new PseudoRandomFactorInitializer(numFactors)
}
