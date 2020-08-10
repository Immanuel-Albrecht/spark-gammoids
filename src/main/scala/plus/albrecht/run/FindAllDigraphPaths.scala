package plus.albrecht.run

import plus.albrecht.digraphs.spark.DigraphFamily
import plus.albrecht.matroids.NamedMatroid
import plus.albrecht.matroids.spark.SparkBasisMatroid
import plus.albrecht.tests.TestResult

import org.apache.spark.sql.functions.lit

/**
  * loads a family of digraphs from an external source,
  * produces the family of all paths in those digraphs,
  * and stores the result to another external source.
  */
object FindAllDigraphPaths {

  /**
    * In sbt interactive prompt, use 'run plus.albrecht.run.TestMain'
    * to call this function.
    *
    * @param args list of matroid names...
    */
  def main(args: Array[String]): Unit = {

    Config(_.setTagSet(Set("spark")).set("app-name", "FindAllDigraphPaths"))

    if (args.length < 2) {
      println(
        "ERROR: we expect exactly two arguments: first, the location" +
          "of the digraph family parquet, and second, the target location for the" +
          "arc family parquet (overwrite mode on)."
      )
      System.exit(1)
    }

    val source = args(0)
    val target = args(1)

    println("Determining all paths in digraphs")
    println(s"  - loaded from ${source} (parquet)")
    println(s"  - writing result to ${target} (parquet, overwrite).")

    DigraphFamily[Int, Int](source, "parquet", lit(true)).df_allPaths.write
      .mode("overwrite")
      .parquet(target)

    println("Done.")

  }

}
