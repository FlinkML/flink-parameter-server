package hu.sztaki.ilab.ps.passive.aggressive.algorithm.binary

import hu.sztaki.ilab.ps.passive.aggressive.entities.SparseVector

import scala.collection.immutable.HashMap

object PassiveAggressiveFilter {
    def buildPAF(): PassiveAggressiveFilter  = new PassiveAggressiveFilterImp()
    def buildPAFI(Con :Int): PassiveAggressiveFilter = new PassiveAggressiveFilterImpI(Con)
    def buildPAFII(Con :Int): PassiveAggressiveFilter = new PassiveAggressiveFilterImpII(Con)
}

abstract class PassiveAggressiveFilter(C :Int) extends Serializable {
  
  protected def Const = C

  protected def getTau(data: SparseVector[Double], l: Double) : Double

  def delta(data: SparseVector[Double], model: HashMap[Long, Double], label: Int) = {
    assert(Set(1, -1) contains label)
    assert(data.getIndexes == model.keySet)
//    suffer loss:
    val l = math.max(0, 1 - label * model.merged(data.getValues)({ case ((k,v1),(_,v2)) => (k,v1*v2) }).values.sum)
    val multiplier = getTau(data, l) * label
    data.getValues map {case (key, value) => (key, value * multiplier)}
  }

  def predict(data: SparseVector[Double], model: HashMap[Long, Double]) =
    Math.signum(model.merged(data.getValues)({ case ((k,v1),(_,v2)) => (k,v1*v2) }).values.sum).toInt

  protected def quotient(data: SparseVector[Double], l: Double, denominatorConst: Double) = {
    val normSquare = data.getValues.values.map(math.pow(_,2)).sum
    if (denominatorConst == 0) l / normSquare
    else l / (normSquare + denominatorConst)
  }
}

class PassiveAggressiveFilterImp extends PassiveAggressiveFilter(0) {
  override def getTau(data: SparseVector[Double], l: Double): Double = quotient(data, l, 0)

}

class PassiveAggressiveFilterImpI(Con :Int) extends PassiveAggressiveFilter(Con) {
  override def getTau(data: SparseVector[Double], l: Double): Double = Math.min(Const, quotient(data, l, 0))
}

class PassiveAggressiveFilterImpII(Con :Int) extends PassiveAggressiveFilter(Con) {
  override def getTau(data: SparseVector[Double], l: Double): Double = quotient(data, l, 1 / (2 * Const))
}


