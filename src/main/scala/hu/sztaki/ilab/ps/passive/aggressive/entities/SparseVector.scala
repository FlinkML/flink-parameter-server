package hu.sztaki.ilab.ps.passive.aggressive.entities

import scala.collection.immutable.HashMap


object SparseVector {
  def build[E](maxSize: Long, elements: (Long, E)*) =
    new SparseVector[E](HashMap[Long, E](elements: _*), maxSize)

  def build[E](maxSize: Long, elements: Traversable[(Long, E)]) =
    new SparseVector[E](HashMap[Long, E](elements.toSeq: _*), maxSize)

  def endOfFile[E](workerId: Int, minusSourceId: Int) = new EOFSign[E](workerId: Int, minusSourceId: Int)
}


class SparseVector[E](indexs: HashMap[Long, E], maxSize: Long) extends Serializable {
  def get(index: Long) = if (index < maxSize && indexs.contains(index)) indexs(index)
  else throw new IllegalArgumentException("index should be less than a capacity of the Vector")

  def getIndexes = indexs.keySet

  def getValues = indexs

  def size = maxSize

  override def equals(that: Any): Boolean =
    that match {
      case that: SparseVector[E] => this.maxSize == that.size && this.indexs == that.getValues
      case _ => false
    }

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + maxSize.toInt;
    result = prime * result + (if (indexs == null) 0 else indexs.hashCode)
    return result
  }
}

case class EOFSign[E](workerId: Int, minusSourceId: Int) extends SparseVector[E](null, 0L)
