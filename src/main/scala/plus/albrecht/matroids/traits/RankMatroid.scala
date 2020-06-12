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
   * @param x   set of matroid elements
   * @return the rank of the set x
   */
  def rk(x : Iterable[T]) : Int
}
