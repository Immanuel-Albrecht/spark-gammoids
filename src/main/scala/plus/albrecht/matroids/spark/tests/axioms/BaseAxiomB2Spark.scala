package plus.albrecht.matroids.spark.tests.axioms

import org.apache.spark.sql.ColumnName
import org.apache.spark.sql.functions._
import plus.albrecht.matroids.spark.SparkBasisMatroid
import plus.albrecht.matroids.spark.traits.SparkMatroid
import plus.albrecht.matroids.tests.axioms.traits
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
   *
   **/
  override lazy val result: TestResult = {

    /*
      (B2) If B1 and B2 are bases and x \in B1-B2, then there is some
           element y \in B2-B1 such that B1-{x}+{y} is a base

     */

    /**
     * get the bases as sets
     *
     * This will break if you have a matroid where you cannot store a single
     * basis on a node */
    val dfBases = (M.dfBasisFamily
      .groupBy(SparkMatroid.colBfId)
      .agg(sort_array(collect_set(SparkMatroid.colBfElement)).as("B1"))
      .select("B1"))

    val B1 = new ColumnName("B1")
    val B2 = new ColumnName("B2")
    val B3 = new ColumnName("B3")
    val x = new ColumnName("x")
    val y = new ColumnName("y")

    /* base pairs to check */
    val dfB1B2 = (
      dfBases
        .crossJoin(dfBases.withColumnRenamed("B1", "B2"))
        .filter(B1 =!= B2)
      )

    val dfB1B2xy = (dfB1B2
      .withColumn("x", explode(array_except(B1, B2)))
      .withColumn("y", explode(array_except(B2, B1))))

    val dfB1B2xB3 = (
      dfB1B2xy
        .withColumn(
          "B3",
          sort_array(array_union(array_remove(B1, x), array(y)))
        )
        .select(B1, B2, x, B3)
      )

    val valid = new ColumnName("valid")

    val dfB1B2xB3_valid =
      dfB1B2xB3
        .join(
          dfBases
            .withColumnRenamed("B1", "B3")
            .withColumn("isBase", lit(1)),
          Seq("B3"),
          "left"
        )
        .withColumn(
          "valid",
          when(new ColumnName("isBase").isNotNull, lit(1)).otherwise(lit(0))
        )
        .select(B1, B2, x, valid)
        .groupBy(B1, B2, x)
        .agg(sum(valid).as("valid"))

    val counter_ex =
      dfB1B2xB3_valid
        .filter(valid === 0)
        .select(B1, B2, x)
        .cache()

    val fast_counter_ex =
      (if (failFast) counter_ex.limit(1) else counter_ex).cache()

    val wrong = fast_counter_ex.count

    val result =
      if (wrong == 0)
        TestResult("[v] axiom (B2) holds.")
      else
        TestResult(
          s"[x] There are ${if (failFast) "at least " else ""}${wrong} counter-examples to (B2)." +
            s" Counter-Example: ${fast_counter_ex.head.toString} = [B1,B2,x]"
        )

    fast_counter_ex.unpersist()

    result
  }
}
