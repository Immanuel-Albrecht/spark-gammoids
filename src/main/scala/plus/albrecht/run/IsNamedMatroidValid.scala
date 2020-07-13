package plus.albrecht.run

import plus.albrecht.matroids.NamedMatroid
import plus.albrecht.matroids.spark.SparkBasisMatroid
import plus.albrecht.tests.TestResult

/**
 * runs a test whether the named matroids given as parameters are valid
 */
object IsNamedMatroidValid {

  /**
   * In sbt interactive prompt, use 'run plus.albrecht.run.TestMain' to call this function.
   *
   * @param args list of matroid names...
   */
  def main(args: Array[String]): Unit = {

    Config(_.setTagSet(Set("spark")).set("app-name","IsNamedMatroidValid"))

    args.foreach(name ⇒ {
      val M = SparkBasisMatroid(NamedMatroid(name))

      println(s"Is ${name} valid? Working on it...")
      val result: TestResult = M.isValid()

      println(f"Is ${name} valid? ${if (result.passed) "yes" else "no"}")
      println(f"Report:")
      println(f"=======")
      result.remarks.foreach(x ⇒ println(f"  ${x}"))
    })
  }
}
