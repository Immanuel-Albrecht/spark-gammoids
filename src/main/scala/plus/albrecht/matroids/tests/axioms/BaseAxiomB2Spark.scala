package plus.albrecht.matroids.tests.axioms

import org.apache.spark.sql.ColumnName
import org.apache.spark.sql.functions._
import plus.albrecht.matroids.SparkBasisMatroid
import plus.albrecht.matroids.traits.SparkMatroid
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
    * Well, also, rework this; it's probably quite wrong:
    *
    * Is P8pp valid? no
    * Report:
    * =======
    * [v] 4 is a valid rank.
    * [v] There is a basis.
    * [v] 0 bases have the wrong cardinality.
    * [v] 0 base elements are non-matroid elements.
    * [x] There are at least 1 counter-examples to (B2).
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
      .agg(collect_set(SparkMatroid.colBfElement).as("B1"))
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
        .withColumn("B3", array_union(array_remove(B1, x), array(y)))
        .select(B1, B2, x, B3)
      )

    val dfB1B2xB3_valid =
      dfB1B2xB3.join(dfBases.withColumnRenamed("B1", "B3"), Seq("B3"), "left")

    val dfB1B2x_count =
      dfB1B2xB3_valid.groupBy(B1, B2, x).agg(count(B3).as("nbr"))

    val counter_ex = dfB1B2x_count.filter(new ColumnName("nbr") === 0)

    //counter_ex.show()

    val wrong = (if (failFast) counter_ex.limit(1) else counter_ex).count

    if (wrong == 0)
      TestResult("[v] axiom (B2) holds.")
    else
      TestResult(
        s"[x] There are ${if (failFast) "at least " else ""}${wrong} counter-examples to (B2)."
      )
  }
}
