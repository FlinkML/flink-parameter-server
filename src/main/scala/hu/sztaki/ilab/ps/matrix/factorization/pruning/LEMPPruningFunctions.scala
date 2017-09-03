package hu.sztaki.ilab.ps.matrix.factorization.pruning

import hu.sztaki.ilab.ps.matrix.factorization.utils.Utils.ItemId
import hu.sztaki.ilab.ps.matrix.factorization.utils.Vector._

/**
  * Implementations for LEMP Pruning functions from this paper:
  * https://ub-madoc.bib.uni-mannheim.de/40018/1/teflioudi15lemp.pdf
  */
object LEMPPruningFunctions {

  /**
    * A function that implements LEMP LENGTH pruning
    *
    * @param minLengthSqr
    * The minimum square length of the vectors to include in the candidate set
    * @return
    * A filter function that selects vectors based on this pruning strategy
    */
  def lengthPruning(minLengthSqr: Double)(v: (ItemId, LengthAndVector)): Boolean = {
    v._2._1 * v._2._1 >= minLengthSqr
  }

  /**
    * A function that implements LEMP COORD pruning
    *
    * @param f
    * The index of the focus coordinate
    * @param userVector
    * The user vector with its length
    * @param theta_b_q
    * \theta_b(\mathbf q) (see the related paper)
    * @return
    * A filter function that selects vectors based on this pruning strategy
    */
  def coordPruning(f: Int, userVector: LengthAndVector, theta_b_q: Double): ((ItemId, (VectorLength, Vector))) => Boolean = {
    val (l_f, u_f) = {
      val q_bar_f = userVector._2(f) / userVector._1
      val a = q_bar_f * theta_b_q
      val b = Math.sqrt((1 - theta_b_q * theta_b_q) * (1 - q_bar_f * q_bar_f))
      val L_f_prime = a - b
      val U_f_prime = a + b
      (if ((q_bar_f >= 0) || (L_f_prime > theta_b_q / q_bar_f)) L_f_prime else -1.0,
        if ((q_bar_f <= 0) || (U_f_prime < theta_b_q / q_bar_f)) U_f_prime else 1.0)
    }
    p: (ItemId, LengthAndVector) =>
      val p_bar_f = p._2._2(f) / p._2._1
      (l_f <= p_bar_f) && (p_bar_f <= u_f)
  }

  /**
    * A function that implements LEMP INCR pruning
    *
    * @param F
    * An array containing the indices of focus coordinates
    * @param user
    * The user vector with its length
    * @param theta
    * \theta (see the related paper)
    * @return
    * A filter function that selects vectors based on this pruning strategy
    */
  def incrPruning(F: Array[Int], user: LengthAndVector, theta: Double): ((ItemId, (VectorLength, Vector))) => Boolean = {
    val n = F.length // number of factors we prune by
    val q_mF_sqr = { // $||\mathbf q_{-F}||^2$ (not normalised)
      var i = 0
      var q_F_sqr = 0.0
      while (i < n) {
        q_F_sqr += user._2(F(i)) * user._2(F(i))
        i += 1
      }
      user._1 * user._1 - q_F_sqr
    }
    p: (ItemId, LengthAndVector) =>
      var i = 0
      var q_F_p_F = 0.0 // $\mathbf q_F^T\mathbf p_F$ (not normalised)
    var p_F_sqr = 0.0 // $||\mathbf p_F||^2$ (also not normalised)
      while (i < n) {
        q_F_p_F += user._2(F(i)) * p._2._2(F(i))
        p_F_sqr += p._2._2(F(i)) * p._2._2(F(i))
        i += 1
      }
      // Inequality (5) is rearranged, to avoid square roots, as
      // $$||\mathbf q_{-F}||^2 (||\mathbf p||^2-||\mathbf p_F||^2) \ge \left(\theta-\mathbf q_F^T\mathbf p_F\right)^2

      // The right hand side of the inequality is the square of this (need to check if positive)
      val u_bound = theta - q_F_p_F
      (u_bound < 0.0) || (q_mF_sqr * (p._2._1 * p._2._1 - p_F_sqr) >= u_bound * u_bound)
  }
}


