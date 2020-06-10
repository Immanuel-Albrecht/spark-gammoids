package plus.albrecht.matroids.traits

import plus.albrecht.tests.TestResult

/**
 * base trait for all matroid structures
 *
 * @tparam T  matroid element type
 */
trait Matroid[T] {
  /**
   *
   * @return iterable of the elements that make up the ground set of the matroid
   */
  def groundSet() : Iterable[T]

  /**
   * Tests whether a given set is a subset of the ground set of the matroid
   * @param set
   * @return true if (set subsetOf groundSet)
   */
  def isSubset(set : Iterable[T]) : Boolean = {
    set.toSet subsetOf groundSet().toSet
  }

  /**
   *
   * @return the rank of the matroid
   */
  def rank() : Int

  /**
   * Tests whether the data underlying the object that implements this trait
   * is all okay and this object really represents a desired kind of matroid.
   *
   * @param failFast   if true, then return the result as soon as it is clear
   *                   that the object is not a valid matroid
   * @return the test result
   */
  def isValid(failFast : Boolean) : TestResult

  /**
   * Tests whether the data underlying the object that implements this trait
   * is all okay and this object really represents a desired kind of matroid.
   *
   * Will return failure as early as possible.
   *
   * @return the test result with failFast
   */
  def isValid() : TestResult = isValid(true)
}
