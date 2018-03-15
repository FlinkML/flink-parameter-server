package hu.sztaki.ilab.ps.sketch.utils

import scala.collection.mutable

object Utils {
  type Vector = Array[Int]

  /**
    * Returns the dot product of two vectors
    *
    * @param u
    * The first vector
    * @param v
    * The second vector
    * @return
    * The dot product of u and v
    */
  def dotProduct(u: Vector, v: Vector): Int = {
    var res = 0
    var i = 0
    val n = u.length  // assuming u and v have the same number of factors
    while (i < n) {
      res += u(i) * v(i)
      i += 1
    }
    res
  }

  def bloomEq(m: Double, k: Double, size: Double): Double = {
    - m / k * math.log(1 - size / m)
  }

  def bloomUnion(m: Double, k: Double, A: mutable.BitSet, B: mutable.BitSet): Double = {
    val union = A | B

    bloomEq(m, k, union.size)
  }

  def bloomUnion(m: Double, k: Double, A: Array[Int], B: Array[Int]): Double = {
    val union = A.toSet | B.toSet

    bloomEq(m, k, union.size)
  }
}

