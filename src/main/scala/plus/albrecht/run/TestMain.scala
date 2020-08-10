package plus.albrecht.run

/**
  * Main object for testing/developing purposes.
  */
object TestMain {

  /**
    * In sbt interactive prompt, use 'run plus.albrecht.run.TestMain'
    * to call this function.
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val is_test = (args contains "--unit-test")

  }
}
