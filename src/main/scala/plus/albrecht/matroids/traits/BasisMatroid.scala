package plus.albrecht.matroids.traits

import plus.albrecht.matroids.tests.axioms.BaseAxiomB2
import plus.albrecht.tests.TestResult

/**
 * base trait for matroids defined through their bases
 *
 * @tparam T matroid element type
 */
trait BasisMatroid[T] extends Matroid[T] {

  /**
   *
   * @return iterable of all the bases of the matroid
   */
  def basisFamily(): Iterable[Set[T]]

  /**
   * check whether a given set is a basis
   *
   * @param set set to check for basis property
   *
   * @return True, if set is indeed a basis of the matroid
   */
  def isBasis(set: Iterable[T]): Boolean

  /**
   * check whether a given set is a basis of the dual matroid
   *
   * @param set set to check for basis property
   *
   * @return True, if set is indeed a basis of the dual of this matroid
   */
  def `isBasis*`(set :Iterable[T]):Boolean = {
    val `b*` = groundSetAsSet.diff(set.toSet)
    /* the bases of the dual matroid are the complements of the bases of the primal matroid */
    isBasis(`b*`)
  }

  /**
   * lazy baseAxiomB2Test; we always fail fast, because counterexamples come
   * in flocks if they exist
   */
  lazy val baseAxiomB2Test = new BaseAxiomB2(this)

  /**
   * lazy test for non-negative matroid rank
   */
  lazy val rankNonNegativeTest: TestResult = {
    if (rank() < 0)
      TestResult(false, List(f"[x] The rank is ${rank()} < 0."))
    else
      TestResult(true, List(f"[v] ${rank()} is a valid rank."))
  }

  /** lazy test: non-empty basis family? */

  lazy val nonEmptyBasisFamilyTest: TestResult = {
    if (basisFamily().size < 1)
      TestResult(false, List("[x] Basis family is empty violating the basis existence axiom."))
    else
      TestResult(true, List("[v] There is a basis."))
  }

  /** lazy test:  have all bases the right cardinality? */

  lazy val basesCardinalityTest: TestResult = {
    val wrong = basisFamily().foldLeft(0)({
      case (count, basis) ⇒
        if (basis.size == rank())
          count
        else
          count + 1
    })
    TestResult(wrong == 0, List(f"${if (wrong == 0) "[v]" else "[x]"} ${wrong} bases have the wrong cardinality."))
  }

  /** lazy test: are the bases all in the ground set? */

  lazy val basesInMatroidTest: TestResult = {
    val wrong = basisFamily().foldLeft(0)({
      case (count, basis) ⇒
        if (isSubset(basis))
          count
        else
          count + 1
    })
    TestResult(wrong == 0, List(f"${if (wrong == 0) "[v]" else "[x]"} ${wrong} bases have non-matroid elements."))
  }

  /**
   * Tests whether:
   *   - the rank is valid
   *   - the family of bases is nonempty
   *   - the bases have the right cardinality
   *
   * @param failFast if true, then return the result as soon as it is clear
   *                 that the object is not a valid matroid
   *
   * @return the test result
   */
  override def isValid(failFast: Boolean): TestResult = {
    /* here go the tests */
    val tests: List[() ⇒ TestResult] = List(
      () ⇒ rankNonNegativeTest,
      () ⇒ nonEmptyBasisFamilyTest,
      () ⇒ basesCardinalityTest,
      () ⇒ basesInMatroidTest,
      () ⇒ baseAxiomB2Test.result // This is a more complicated test with its own object.
    )

    /* apply the tests one after another */
    tests.foldLeft(TestResult(true, List()))({
      case (last_result, next_test) ⇒ {
        if (failFast && (!last_result.passed)) last_result
        else last_result ++ next_test()
      }
    })
  }
}
