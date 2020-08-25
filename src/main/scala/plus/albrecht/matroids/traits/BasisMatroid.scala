package plus.albrecht.matroids.traits

import plus.albrecht.algorithms.FindExtremalLinearOrder
import plus.albrecht.iterables.OrderedMChooseN
import plus.albrecht.matroids.tests.axioms.BaseAxiomB2
import plus.albrecht.matroids.tests.axioms.traits.AxiomTest
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
  def `isBasis*`(set: Iterable[T]): Boolean = {
    val `b*` = groundSetAsSet.diff(set.toSet)
    /* the bases of the dual matroid are the complements of the bases of the primal matroid */
    isBasis(`b*`)
  }

  /**
    * lazy baseAxiomB2Test; we always fail fast, because counterexamples come
    * in flocks if they exist
    *
    * override this if you want a different implementation of the test
    */
  lazy val baseAxiomB2Test: AxiomTest = new BaseAxiomB2(this)

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
      TestResult(
        false,
        List("[x] Basis family is empty violating the basis existence axiom.")
      )
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
    TestResult(
      wrong == 0,
      List(
        f"${if (wrong == 0) "[v]" else "[x]"} ${wrong} bases have " +
          f"the wrong cardinality."
      )
    )
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
    TestResult(
      wrong == 0,
      List(
        f"${if (wrong == 0) "[v]" else "[x]"} ${wrong} bases have " +
          f"non-matroid elements."
      )
    )
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
      () ⇒
        baseAxiomB2Test.result // This is a more complicated test with its own object.
    )

    /* apply the tests one after another */
    tests.foldLeft(TestResult(true, List()))({
      case (last_result, next_test) ⇒ {
        if (failFast && (!last_result.passed)) last_result
        else last_result ++ next_test()
      }
    })
  }

  /**
    * computes the partial basis indicator vector
    *
    * @param partialOrder   elements to consider, in given implicit order
    * @return  partial basis indicator vector
    */
  def partialBasisIndicatorVector(partialOrder: Seq[T]) = {
    OrderedMChooseN(partialOrder.size, rank()).map(elts ⇒
      isBasis(elts.map(partialOrder(_)))
    )
  }

  /**
    * computes the partial basis indicator vector of the dual matroid
    *
    * @param partialOrder   elements to consider, in given implicit order
    * @return  partial basis indicator vector
    */
  def `partialBasisIndicatorVector*`(partialOrder: Seq[T]) = {
    OrderedMChooseN(partialOrder.size, `rank*`()).map(elts ⇒
      `isBasis*`(elts.map(partialOrder(_)))
    )
  }

  /**
    * the co-canonical order of the ground set of elements
    * minimizes the basis-indicator-vector with respect to the
    * lexicographic order on the rank-sized subsets (which are
    * ordered in reverse lexicographic order)
    */
  lazy val cocanonicalOrdering = FindExtremalLinearOrder(
    groundSetAsSet,
    groundSetAsSet
      .subsets(rank())
      .map(b ⇒ b.toList.permutations.map(_.toSeq).toIterable)
      .toSeq,
    partialBasisIndicatorVector(_),
    groundSetAsSet.size - rank(),
    findMaximum = false
  )

  /**
    * the canonical order of the ground set of elements
    * maximizes the basis-indicator-vector with respect to the
    * lexicographic order on the rank-sized subsets (which are
    * ordered in reverse lexicographic order)
    */
  lazy val canonicalOrdering = FindExtremalLinearOrder(
    groundSetAsSet,
    basisFamily.map(b ⇒ b.toList.permutations.map(_.toSeq).toIterable).toSeq,
    partialBasisIndicatorVector(_),
    groundSetAsSet.size - rank(),
    findMaximum = true
  )

  /**
    * the canonical order of the ground set of elements
    * maximizes the basis-indicator-vector of the dual matroid
    * with respect to the
    * lexicographic order on the rank-sized subsets (which are
    * ordered in reverse lexicographic order)
    */
  lazy val `canonicalOrdering*` = FindExtremalLinearOrder(
    groundSetAsSet,
    basisFamily
      .map(b ⇒
        (groundSetAsSet.diff(b)).toList.permutations.map(_.toSeq).toIterable
      )
      .toSeq,
    `partialBasisIndicatorVector*`(_),
    rank(),
    findMaximum = true
  )

  /**
    * the co-canonical basis indicator vector is an iso-class invariant of
    * the matroid, and can be used to identify a matroid up to isomorphism.
    *
    * Although it is close to Lukas Finschi's database of oriented matroids,
    * the fact that bringing a non-base farther to the right when a negative
    * orientation of a basis may be changed to a positive implies that you
    * cannot just use this for checking whether a given matroid is orientable.
    *
    * RevLex-Index (Finschi O.M. db)
    *
    * For a given rank r and a given number n of elements,
    * the RevLex-Index uniquely identifies isomorphism classes
    * of oriented matroids, or abstract order types, or abstract dissection
    * types of rank r with n elements. The index is based on the representation
    * of oriented matroids by chirotopes, where the signs of the bases are
    * ordered in reverse lexicographic order, and the representative is the
    * oriented matroid in the corresponding equivalence class with
    * lexicographically maximal chirotope, where - < + < 0.
    */
  lazy val cocanonicalBasisIndicator = partialBasisIndicatorVector(
    cocanonicalOrdering
  )

  /**
    * the canonical basis indicator vector is an iso-class invariant of
    * the matroid, and can be used to identify a matroid up to isomorphism.
    *
    */
  lazy val canonicalBasisIndicator = partialBasisIndicatorVector(
    canonicalOrdering
  )

  /**
    * canonical basis indicator of the dual matroid
    *
    * the canonical basis indicator vector is an iso-class invariant of
    * the matroid, and can be used to identify a matroid up to isomorphism.
    *
    */
  lazy val `canonicalBasisIndicator*` = `partialBasisIndicatorVector*`(
    `canonicalOrdering*`
  )

  /**
    * the canonicalBasisIndicator written down as a string of . and X's;
    * where an 'X' indicates a basis, and an '.' indicates a non-basis.
    */
  lazy val basisIndicatorString: String = {
    canonicalBasisIndicator.foldLeft("")({
      case (s0, is_basis) ⇒ {
        s0 + (if (is_basis) 'X' else '.')
      }
    })
  }

  /**
    * the canonicalBasisIndicator* written down as a string of . and X's;
    * where an 'X' indicates a basis, and an '.' indicates a non-basis.
    */
  lazy val `basisIndicatorString*` : String = {
    `canonicalBasisIndicator*`.foldLeft("")({
      case (s0, is_basis) ⇒ {
        s0 + (if (is_basis) 'X' else '.')
      }
    })
  }

  /**
    * tests whether this matroid is isomorphic to its dual
    * @return true, if this matroid is self-dual.
    */
  def isSelfDual(): Boolean = {
    if (rank() != `rank*`()) false
    else
      canonicalBasisIndicator == `canonicalBasisIndicator*`
  }

  /**
    *
    *
    * @return default string representation of this BasisMatroid
    */
  override def toString(): String =
    s"${groundSet().size},${rank},${basisIndicatorString}"
}
