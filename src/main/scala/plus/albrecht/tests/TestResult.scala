package plus.albrecht.tests

/**
 *
 * @param passed  flag whether the test has been passed
 *
 * @param remarks a list of remarks/comments that have been uttered when the test was done
 */
case class TestResult(val passed: Boolean, val remarks: List[String]) {

  /**
   * combine the results of two tests into one bigger test result
   *
   * @param next_test_result the result of the other test
   *
   * @return combined test result
   */
  def ++(next_test_result: TestResult): TestResult = {
    new TestResult(passed && (next_test_result.passed), remarks ++ next_test_result.remarks)
  }

  /**
   * This is here, so we can do isValid() on a test result that is actually just
   * a lazy value.
   *
   * @return this
   */
  def apply(): TestResult = this

  /**
   * This is here, so we can do isValid(true) on a test result that is actually just
   * a lazy value.
   *
   * @return this
   */
  def apply(failFast: Boolean): TestResult = this

}

/**
 * convenience companion
 */
object TestResult {
  /**
   * convenience function so we may write TestResult("[v] test succeeded.")
   * or TestResult("[x] there's something wrong.")
   *
   * @param single_remark remark from testing
   *
   * @return TestResult object
   *
   */
  def apply(single_remark: String): TestResult = {
    new TestResult(single_remark.startsWith("[v]"), single_remark :: Nil)
  }
}
