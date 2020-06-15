package plus.albrecht.matroids.tests.axioms

import plus.albrecht.matroids.SparkBasisMatroid
import plus.albrecht.tests.TestResult

class BaseAxiomB2Spark[T](val M: SparkBasisMatroid[T],
                          override val failFast: Boolean)
  extends traits.AxiomTest {

  /**
   * convenience constructor
   *
   * @param m basis matroid under consideration
   */
  def this(m: SparkBasisMatroid[T]) {
    this(m, true)
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
    TestResult("[x] NOT IMPLEMENTED!")
  }
}
