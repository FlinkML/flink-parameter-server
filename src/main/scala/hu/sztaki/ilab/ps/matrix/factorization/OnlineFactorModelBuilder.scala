package hu.sztaki.ilab.ps.matrix.factorization

import hu.sztaki.ilab.ps.matrix.factorization.factors.{FactorInitializerDescriptor, FactorUpdater}

trait OnlineFactorModelBuilder[Stream[_], RatingType, VectorType] extends Serializable {

  def buildModel(ratings: Stream[RatingType],
                 factorInit: FactorInitializerDescriptor,
                 factorUpdate: FactorUpdater,
                 parameters: Map[String, String] = Map.empty): Stream[(VectorType, VectorType)]

}
