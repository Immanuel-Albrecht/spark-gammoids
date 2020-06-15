package plus.albrecht.matroids.tests.axioms.traits

import plus.albrecht.tests.TestResult

/**
 * base trait for any kind of axiom test
 */
trait AxiomTest {

  /**
   * Test mode
   */
  val failFast: Boolean

  /**
   * does it hold? --> override this!
   */
  lazy val result: TestResult = new TestResult(false,
    List("Please override this with your own test implementation!"))

}
