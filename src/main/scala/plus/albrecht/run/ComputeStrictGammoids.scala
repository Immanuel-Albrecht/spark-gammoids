package plus.albrecht.run

import org.apache.spark.sql.functions.{col, lit}
import plus.albrecht.digraphs.spark.{CollectionOfDigraphPaths, DigraphFamily}

/**
  * loads a family of path-sets from digraphs from an external source,
  * produces the family of strict gammoids that correspond to the underlying
  * digraphs and stores the result to another external source.
  */
object ComputeStrictGammoids {

  /**
    * In sbt interactive prompt, use 'run plus.albrecht.run.TestMain'
    * to call this function.
    *
    * @param args list of matroid names...
    *
    *  Parameter list:
    *
    *  mandatory:
    *    args(0)   ... path to source parquet
    *    args(1)   ... path to target parquet (is overwritten)
    *
    *
    *  optional:
    *    args(2)    ... inclusive lower limit for ID in digraph family
    *    args(3)    ... upper limit (excluded) for ID in digraph family
    *
    */
  def main(args: Array[String]): Unit = {

    Config(_.setTagSet(Set("spark")).set("app-name", "ComputeStrictGammoids"))

    if (args.length < 2) {
      println(
        "ERROR: we expect exactly two arguments: first, the location" +
          "of the path set family parquet, and second, the target location for the" +
          "arc family parquet (overwrite mode on)."
      )
      System.exit(1)
    }

    val source = args(0)
    val target = args(1)

    val filter = if (args.length >= 4) {
      (col(DigraphFamily.id) >= lit(args(2).toLong)) &&
      (col(DigraphFamily.id) < lit(args(3).toLong))
    } else lit(true)

    println("Computing all strict gammoids from path structures")
    println(s"  - loaded from ${source} (parquet)")
    println(s"    - with filter: ${filter}")
    println(s"  - writing result to ${target} (parquet, overwrite).")

    CollectionOfDigraphPaths[Int, Long](
      source,
      "parquet",
      filter
    ).df_gammoids.write
      .mode("overwrite")
      .partitionBy(
        CollectionOfDigraphPaths.nbrElements,
        CollectionOfDigraphPaths.rank
      )
      .parquet(target)

    println("Done.")

  }

}
