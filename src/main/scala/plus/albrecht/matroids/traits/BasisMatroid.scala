package plus.albrecht.matroids.traits

import plus.albrecht.matroids.tests.axioms.BaseAxiomB2
import plus.albrecht.tests.TestResult

/**
 * base trait for matroids defined through their bases
 *
 * @tparam T  matroid element type
 */
trait BasisMatroid[T] extends Matroid[T]{

  /**
   *
   * @return iterable of all the bases of the matroid
   */
  def basisFamily() : Iterable[Set[T]]

  /**
   * check whether a given set is a basis
   *
   * @param set   set to check for basis property
   * @return True, if set is indeed a basis of the matroid
   */
  def isBasis(set : Iterable[T]) : Boolean


  /**
   * lazy baseAxiomB2Test; we always fail fast, because counterexamples come
   * in flocks if they exist
   */
  lazy val baseAxiomB2Test = new BaseAxiomB2(this)

  /**
   * Tests whether:
   *   - the rank is valid
   *   - the family of bases is nonempty
   *   - the bases have the right cardinality
   *   - the bases are in the ground set
   *
   * @param failFast   if true, then return the result as soon as it is clear
   *                   that the object is not a valid matroid
   * @return the test result
   */
  override def isValid(failFast : Boolean) : TestResult = {
    /* here go the tests */
    val tests : List[() ⇒ TestResult] = List(
      /* is the rank non-negative */
      () ⇒ {
        if (rank() < 0)
          TestResult(false,Array(f"[x] The rank is ${rank()} < 0."))
        else
          TestResult(true,Array(f"[v] ${rank()} is a valid rank."))
      },
      /* non-empty basis family? */
      () ⇒ {
        if (basisFamily().size < 1)
          TestResult(false, Array("[x] Basis family is empty violating the basis existence axiom."))
        else
          TestResult(true, Array("[v] There is a basis."))
      },
      /* have all bases the right cardinality? */
      () ⇒ {
        val wrong = basisFamily().foldLeft(0)({
          case (count, basis) ⇒
            if (basis.size == rank())
              count
            else
              count + 1
        })
        TestResult(wrong == 0,Array(f"${if(wrong==0)"[v]" else "[x]"} ${wrong} bases have the wrong cardinality."))
      },
      /* are the bases all in the ground set? */
      () ⇒ {
        val wrong = basisFamily().foldLeft(0)({
          case (count, basis) ⇒
            if (isSubset(basis))
              count
            else
              count + 1
        })
        TestResult(wrong == 0,Array(f"${if(wrong==0)"[v]" else "[x]"} ${wrong} bases have non-matroid elements."))
      },
      /* finally, test (B2) */
      () ⇒ {baseAxiomB2Test.result}

    )

    /* apply the tests one after another */
    tests.foldLeft(TestResult(true,Array()))({
      case (last_result, next_test) ⇒ {
        if (failFast && (!last_result.passed)) last_result
        else last_result ++ next_test()
      }
    })
  }
}
