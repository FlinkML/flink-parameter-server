package hu.sztaki.ilab.ps.passive.aggressive.algorithm

// FIXME we might not need a separate object for such simple code
object PassiveAggressiveParameterInitializer {

  /**
    * Initializer every parameter with 0.
    */
  val init: Int => Double = _ => 0

}
