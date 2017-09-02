package hu.sztaki.ilab.ps.matrix.factorization.utils

object Vector {

  /**
   * The vector type used in matrix factorization
   */
  type Vector = Array[Double]
  
  /**
   * Length of vector
   */
  type VectorLength = Double
  
  /**
   * A vector with its length. Used by pruning algorithms in LEMP TopK
   */
  type LengthAndVector = (VectorLength, Vector)

  /**
   * Returns the squared length of a vector
   * 
   * @param v
   * The input vector
   * @return
   * The squared length of v
   */
  def vectorLengthSqr(v: Vector): Double = {
    var res = 0.0
    var i = 0
    val n = v.length
    while (i < n) {
      res += v(i) * v(i)
      i += 1
    }
    res
  }
  
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
  def dotProduct(u: Vector, v: Vector): Double = {
    var res = 0.0
    var i = 0
    val n = u.length  // assuming u and v have the same number of factors
    while (i < n) {
      res += u(i) * v(i)
      i += 1
    }
    res
  }
  
  /**
   * Returns the sum of two vectors
   * 
   * @param u
   * The first vector
   * @param v
   * The second vector
   * @return
   * The sum off u and v
   * @throws FactorIsNotANumberException
   * If the operation results in a vector with a NaN coordinate
   */
  def vectorSum(u: Vector, v: Vector): Array[Double] = {
    val n = u.length
    val res = new Array[Double](n)
    var i = 0
    while (i < n) {
      res(i) = u(i) + v(i)
      if (res(i).isNaN) {
        throw new FactorIsNotANumberException
      }
      i += 1
    }
    res
  }
  
  /**
   * Exception to be thrown when a vector addition results in a NaN
   */
  class FactorIsNotANumberException extends Exception
  
  /**
   * Attaches length to a vector
   * 
   * @param u
   * The input vector
   * @return
   * A LengthAndVector object containing u and its length
   */
  def attachLength(u: Vector): LengthAndVector = (Math.sqrt(vectorLengthSqr(u)), u)
}