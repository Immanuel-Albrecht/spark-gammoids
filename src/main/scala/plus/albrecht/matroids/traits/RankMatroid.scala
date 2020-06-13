package plus.albrecht.matroids.traits

/**
 * base trait for matroids that provide a rank oracle
 *
 * @tparam T matroid element type
 */
trait RankMatroid[T] extends Matroid[T] {

  /**
   * rank function of the matroid
   *
   * @param x set of matroid elements
   *
   * @return the rank of the set x
   */
  def rk(x: Iterable[T]): Int

  /**
   * co-rank function of the matroid,
   *
   * aka. the rank function of the dual matroid
   *
   * @param x set of matroid elements
   *
   * @return the rank of the set x with respect to the dual of this matroid
   */
  def `rk*`(x : Iterable[T]) : Int = {
    val s = x.toSet
    /* rk*(X) = rk(E\X) + |X| - rk(E) */
    rk(groundSetAsSet.diff(s)) + s.size - rank()
  }
}
