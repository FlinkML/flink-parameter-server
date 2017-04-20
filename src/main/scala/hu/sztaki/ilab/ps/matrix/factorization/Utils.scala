package hu.sztaki.ilab.ps.matrix.factorization

object Utils {

  type UserId = Int
  type ItemId = Int

  case class Rating(user: UserId, item: ItemId, rating: Double)

  object Rating {
    def fromTuple(t: (Int,Int,Double)): Rating = Rating(t._1, t._2, t._3)
  }

}
