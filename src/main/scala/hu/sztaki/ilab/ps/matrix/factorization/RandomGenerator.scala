package hu.sztaki.ilab.ps.matrix.factorization

import scala.annotation.tailrec
import scala.util.Random

object RandomGenerator {

  class DiscreteExpGen(random: => Random, lambda: Double, n: Int) extends Serializable {

    @transient
    lazy val rand = Random

    def gen(): Int = nextExpDiscrete(rand, lambda, n)
  }

  trait RatingGen extends Serializable{
    def genRating(): (Int, Int, Double)
  }

  class ExponentialRatingGen(random: => Random, lambda: Double, n: Int, m: Int) extends RatingGen {
    @transient
    lazy val rand = Random

    def genRating(): (Int, Int, Double) =
      (nextExpDiscrete(rand, lambda, n), nextExpDiscrete(rand, lambda, m), 1.0)
  }

  class UniformRatingGen(random: => Random, n: Int, m: Int) extends RatingGen {
    @transient
    lazy val rand = Random

    def genRating(): (Int, Int, Double) =
      (rand.nextInt(n), rand.nextInt(m), 1.0)
  }

  def nextExp(random: Random, lambda: Double): Double = {
    val invExpCDF = (x: Double) => scala.math.log(1 - x) / (-1 * lambda)
    invExpCDF(random.nextDouble())
  }

  @tailrec
  def nextExpDiscrete(random: Random, lambda: Double, n: Int): Int = {
    val x = Math.floor(nextExp(random, lambda) * n).toInt
    if (x > n) {
      println(s"wrong $x")
      nextExpDiscrete(random, lambda, n)
    } else {
      x
    }
  }
}
