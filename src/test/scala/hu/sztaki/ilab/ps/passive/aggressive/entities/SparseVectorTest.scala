package hu.sztaki.ilab.ps.passive.aggressive.entities

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.HashMap

class SparseVectorTest extends FlatSpec with Matchers {

  "SparseVectorBuilder and equal methode" should "be work" in {
    val SVortodox = new SparseVector(HashMap(1L -> "one", 2L -> "two"), 5)
    val SVmap = SparseVector.build(5, HashMap(1L -> "one", 2L -> "two"))
    val SVtraversal = SparseVector.build(5, List(1L -> "one", 2L -> "two"))
    val SVseq = SparseVector.build(5, 1L -> "one", 2L -> "two")
    SVortodox should be(SVmap)
    SVortodox should be(SVtraversal)
    SVortodox should be(SVseq)
  }

  "test" should "be work" in {
    import breeze.linalg._
    val x = DenseVector.zeros[Double](5)
    x(1) = 2
    val y = breeze.linalg.SparseVector.zeros[Double](5)
    y(2) = 3
    val q = x + y
    val w = q :* y

    println(x)
    println(y)
    println(q)
    println(w)
  }

}
