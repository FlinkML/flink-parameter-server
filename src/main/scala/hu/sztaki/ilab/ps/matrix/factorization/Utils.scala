package hu.sztaki.ilab.ps.matrix.factorization

object Utils {

  type UserVector = FactorVector
  type ItemVector = FactorVector

  sealed trait VectorUpdate
  case class UserUpdate(vec: UserVector) extends VectorUpdate
  case class ItemUpdate(vec: ItemVector) extends VectorUpdate

  type UserId = Int
  type ItemId = Int

  case class Rating(user: UserId, item: ItemId, rating: Double)

  object Rating {
    def fromTuple(t: (Int,Int,Double)): Rating = Rating(t._1, t._2, t._3)
  }

  case class FactorVector(id: Int, vector: Array[Double]) {
    override def toString: String = s"FactorVector($id, [${vector.mkString(",")}])"
  }

}
