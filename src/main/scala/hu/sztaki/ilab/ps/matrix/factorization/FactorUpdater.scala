package hu.sztaki.ilab.ps.matrix.factorization

trait FactorUpdater extends Serializable {
  def nextFactors(rating: Double,
                  user: Array[Double],
                  item: Array[Double]): (Array[Double], Array[Double])

  /**
    *
    * @param rating
    * @param user
    * @param item
    * @return
    *         Vector to add to user and item vector respectively.
    */
  def delta(rating: Double,
            user: Array[Double],
            item: Array[Double]): (Array[Double], Array[Double])
}
