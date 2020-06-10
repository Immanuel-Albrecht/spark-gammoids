package plus.albrecht.tests

/**
 *
 * @param passed    flag whether the test has been passed
 * @param remarks   a list of remarks/comments that have been uttered when the test was done
 */
case class TestResult(val passed : Boolean, val remarks : Array[String]) {

  /**
   * combine the results of two tests into one bigger test result
   * @param next_test_result   the result of the other test
   * @return combined test result
   */
  def ++(next_test_result:TestResult) : TestResult = {
    new TestResult(passed && (next_test_result.passed), remarks ++ next_test_result.remarks)
  }

}
