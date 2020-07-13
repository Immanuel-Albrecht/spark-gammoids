package plus.albrecht.matroids.tests.axioms

import plus.albrecht.matroids.traits.BasisMatroid
import plus.albrecht.tests.TestResult

/**
 * This class implements a test whether the basis exchange axiom
 * (B2) [p. 17 in Oxley's Matroid Theory] holds.
 *
 * @param M        basis matroid under consideration
 *
 * @param failFast we recommend to set this to true
 *
 * @tparam T matroid element class
 */
class BaseAxiomB2[T](val M: BasisMatroid[T], override val failFast: Boolean)
  extends traits.AxiomTest {

  /**
   * convenience constructor
   *
   * @param m basis matroid under consideration
   */
  def this(m: BasisMatroid[T]) {
    this(m, true)
  }

  /**
   * this object maps bases of hyperplanes to elements outside
   * of the spanned hyperplane
   */
  lazy val hypo_bases: Map[Set[T], Set[T]] = {
    M.basisFamily()
      .foldLeft[Map[Set[T], Set[T]]](Map())({
        case (hypo_map, basis) ⇒
          basis.foldLeft(hypo_map)({
            case (hypo_map, x0) ⇒
              val B0 = basis - x0

              hypo_map + (B0 → (hypo_map.getOrElse(B0, Set[T]()) + x0))
          })
      })
  }

  /**
   * does it hold?
   */
  override lazy val result: TestResult = {
    /*
      (B2) If B1 and B2 are bases and x \in B1-B2, then there is some
           element y \in B2-B1 such that B1-{x}+{y} is a base
     */

    /* I guess there should be something smarter that stops iterating early
       if failFast is set. At least we skip the inner iteration.
     */
    val errors: List[String] = M
      .basisFamily()
      .foldLeft(List[String]())({
        case (errors, b1) ⇒
          if (failFast && (!errors.isEmpty)) errors
          else {
            M.basisFamily()
              .foldLeft(errors)({
                case (errors, b2) ⇒
                  if (failFast && (!errors.isEmpty)) errors
                  else {
                    /* Test B1 vs B2 */

                    val outElements = b1.diff(b2)
                    val inElements = b2.diff(b1)

                    outElements.foldLeft(errors)({
                      case (errors, x) ⇒
                        if (failFast && (!errors.isEmpty)) errors
                        else {
                          if (hypo_bases
                            .getOrElse(b1 - x, Set[T]())
                            .intersect(inElements)
                            .isEmpty) {
                            /* there is no candidate that extends b1-x to another base */
                            errors ++ List(
                              f"[x] ${b1} - ${x} has no candidate in ${inElements}" +
                                f" " +
                                f"to make another basis. [Augmentation-List: ${
                                  hypo_bases
                                    .getOrElse(b1 - x, Set[T]())
                                }]"
                            )
                          } else errors
                        }
                    })
                  }
              })
          }
      })

    if (errors.isEmpty)
      TestResult(true, List("[v] axiom (B2) holds."))
    else
      TestResult(false, errors)
  }
}
