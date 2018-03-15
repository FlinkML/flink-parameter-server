package hu.sztaki.ilab.ps.sketch

import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.PropertyChecks
import utils.Utils._

import scala.collection.mutable

class UtilsTest extends FlatSpec with PropertyChecks with Matchers {

  "Vector operations" should "give correct answers" in {
    val vec1 = Array(1,2,3,4)
    val vec2 = Array(1,1,1,1)
    val vec0 = Array(0,0,0,0)

    dotProduct(vec1, vec2) should be(10)
    dotProduct(vec1, vec0) should be (0)
  }

  "Equations for a bloom filter" should "give correct answers" in {
    val A = mutable.BitSet(1, 4, 10, 20)

    val numHashes = 3
    val arraySize = 20

    math.abs(bloomEq(arraySize, numHashes, A.size) - 1.48762367542806503844) should be < 0.00001

    val B = mutable.BitSet(1,5,10,14)

    math.abs(bloomUnion(arraySize, numHashes, A, B) - 2.37783295959154919) should be < 0.00001
  }

  "Array based bloom filter eq" should "give the correct answers" in {
    val A = Array(1,4,10,20)

    val numHashes = 3
    val arraySize = 20

    math.abs(bloomEq(arraySize, numHashes, A.length) - 1.48762367542806503844) should be < 0.00001

    val B = Array(1,5,10,14)

    math.abs(bloomUnion(arraySize, numHashes, A, B) - 2.37783295959154919) should be < 0.00001
  }

}
