package plus.albrecht.run

/**
 * Main object for testing/developing purposes.
 */
object TestMain {
  /**
   * In sbt interactive prompt, use 'run plus.albrecht.run.TestMain' to call this function.
   * @param args
   */
  def main(args : Array[String]):Unit = {
    println(s"Arguments: ${args}")
    val spark = Spark.spark
    println(s"Spark: ${spark}")
  }
}
