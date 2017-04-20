package hu.sztaki.ilab.ps.matrix.factorization.factors

/**
  * Holds a function that computes delta updates based on a rating to the corresponding
  * user and an item vectors.
  *
  * E.g. it can describe an SGD update ([[SGDUpdater]]).
  */
trait FactorUpdater extends Serializable {

  /**
    * @param rating
    * Rating.
    * @param user
    * User vector.
    * @param item
    * Item vector.
    * @return
    * Vector to add to user and item vector respectively.
    */
  def delta(rating: Double,
            user: Array[Double],
            item: Array[Double]): (Array[Double], Array[Double])
}
