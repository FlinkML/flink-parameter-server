package hu.sztaki.ilab.ps.matrix.factorization.pruning

/**
  * The base class for LEMP pruning strategies
  */
sealed abstract class LEMPPruningStrategy

/**
  * Indicates the use of the Length-based pruning
  */
final case class LENGTH() extends LEMPPruningStrategy

/**
  * Indicates the use of Coordinate-based pruning
  */
final case class COORD() extends LEMPPruningStrategy

/**
  * Indicates the use of Incremental pruning
  *
  * @param numFocusCoordinates
  * The number of focus coordinates to prune by.
  * Focus coordinates will be the largest coordinates of the user vector
  */
final case class INCR(numFocusCoordinates: Int) extends LEMPPruningStrategy // Incremental pruning

/**
  * Indicates a choice between Length-based and Coordinate-based pruning,
  * depending on the ratio of vectors in each bucket
  *
  * @param algorithmSwitchThreshold
  * Used to switch between pruning algorithms.
  * It is the threshold of the ratio of the smallest and largest vector length in a bucket.
  * It is not the same as the threshold described in section 4.4 of the related paper.
  */
final case class LC(algorithmSwitchThreshold: Double) extends LEMPPruningStrategy // Chooses between LENGTH and COORD pruning in each bucket

/**
  * Indicates a choice between Length-based and Incremental pruning,
  * depending on the ratio of vectors in each bucket
  *
  * @param numFocusCoordinates
  * The number of focus coordinates to prune by.
  * Focus coordinates will be the largest coordinates of the user vector
  * @param algorithmSwitchThreshold
  * Used to switch between pruning algorithms.
  * It is the threshold of the ratio of the smallest and largest vector length in a bucket.
  * It is not the same as the threshold described in section 4.4 of the related paper.
  */
final case class LI(numFocusCoordinates: Int, algorithmSwitchThreshold: Double) extends LEMPPruningStrategy // Chooses between LENGTH and INCR pruning in each bucket

object LEMPPruningStrategy {

  private val LENGTH_REGEX = "length".r
  private val COORD_REGEX = "coord".r
  private val INCR_REGEX = "incr:(\\d*)".r
  private val LC_REGEX = "lc:([0-9\\.]*)".r
  private val LI_REGEX = "li:(\\d*):([0-9\\.]*)".r

  /**
    * Parses a string for a LEMP pruning strategy
    *
    * @param s
    * The input string. Can be "length", "coord", "incr:#", "lc:#", "li:#:#" where the #s mean numbers
    * @return
    * The LEMP Pruning strategy parsed
    */
  def fromString(s:String):LEMPPruningStrategy = s match {
    case LENGTH_REGEX() => LENGTH()
    case COORD_REGEX() => COORD()
    case INCR_REGEX(numFocusCoordinates) => INCR(numFocusCoordinates.toInt)
    case LC_REGEX(threshold) => LC(threshold.toDouble)
    case LI_REGEX(numFocusCoordinates, threshold) => LI(numFocusCoordinates.toInt, threshold.toDouble)
    case _ => throw new IllegalArgumentException(s"Invalid LEMP Pruning strategy string $s")
  }


}