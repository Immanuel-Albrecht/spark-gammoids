package plus.albrecht.matroids.tests.axioms

import org.apache.spark.sql.ColumnName
import org.apache.spark.sql.functions.lit
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
   */
  override lazy val result: TestResult = {

    /*
      (B2) If B1 and B2 are bases and x \in B1-B2, then there is some
           element y \in B2-B1 such that B1-{x}+{y} is a base

       Well, this is also incredibly slow compared to the scala version. But it runs on a cluster :)
     */

    /** dfBasisFamily of the spark matroid */
    val df = M.dfBasisFamily

    /** Data frame with (basis_id,b2,element)-records, where elt is a common element of
     * the bases identified by basis_id and b2.
     *
     * To obtain b1-b2, we use a left-anti-join with this data frame (b2 takes part in it,too.)
     */
    val intersection_elements = (df
      .join(df.withColumnRenamed(SparkMatroid.colBfId,"b2"),
        Seq(SparkMatroid.colBfElement)))

    /** This holds all triples of the kind (x,B1,B2) where x \in B1-B2,
     *   and B1,B2 are both bases.
     *
     *  Column names are b2, basis_id, element
     *  */
    val x_b1_b2 = (df.select(SparkMatroid.colBfId)
      .withColumnRenamed(SparkMatroid.colBfId,"b2")
      .crossJoin(df)
      .join(intersection_elements,Seq(SparkMatroid.colBfElement,
        SparkMatroid.colBfId,"b2"),"leftanti")
      .withColumnRenamed(SparkMatroid.colBfElement,"x"))

    /* At this point, we have a data frame that consists of our premises.
    * All we have to do is filter out those, for which we find an adequate
    * y in B2-B1 such that B3 = B1-{x}+{y} is a basis again. */

    /** B1 and B3 are related such that the intersection has cardinality rank()-1
     * */
    val b1_b3 = (intersection_elements
      .groupBy(SparkMatroid.colBfId,"b2")
        .count.filter(new ColumnName("count").isin(M.rank()-1))
      .select(SparkMatroid.colBfId,"b2").withColumnRenamed("b2","b3"))
    /* we might be tempted to not rename the column b2, as we need this for joining later,
       but I decided that this would be evil madness.
     */

    /**
     * determine the elements x and y in the b3 = b1-{x}+{y} relation
     */
    val b1_x_y = (
      /* first, obtain x */
      b1_b3.join(df,Seq(SparkMatroid.colBfId))
        .withColumnRenamed("b3","b2")
        .join(intersection_elements,
          Seq(SparkMatroid.colBfElement,
            SparkMatroid.colBfId,"b2"),"leftanti")
        .withColumnRenamed(SparkMatroid.colBfElement,"x")
        .withColumnRenamed(SparkMatroid.colBfId,"b1")
        /* second, obtain y */
        .withColumnRenamed("b2",SparkMatroid.colBfId)
        .withColumnRenamed("b1","b2")
        .join(df,Seq(SparkMatroid.colBfId))
        .join(intersection_elements,
          Seq(SparkMatroid.colBfElement,
            SparkMatroid.colBfId,"b2"),"leftanti")
        .withColumnRenamed(SparkMatroid.colBfElement,"y")
        .withColumnRenamed("b2","b1")
      .withColumnRenamed(SparkMatroid.colBfId,"b2")
      .withColumnRenamed("b1",SparkMatroid.colBfId)
        .select(SparkMatroid.colBfId,"x","y")
    ) /* sorry for the column renaming exercise */

    /**
     * determine the candidates y from B2-B1
     *
     */

    val b1_b2_y = (x_b1_b2
      .join(df.withColumnRenamed("basis_id","b2"),Seq("b2"))
      .join(intersection_elements,Seq(SparkMatroid.colBfElement,SparkMatroid.colBfId,"b2"),"leftanti")
        .withColumnRenamed(SparkMatroid.colBfElement,"y")
      .select(SparkMatroid.colBfId,"b2","y")
      )



    /**
     * determine all valid basis exchanges for b1,b2,x where y in b2-b1
     *
     * we use a left join because we are interested in those rows that have no
     * satisfaction of the exists subclause in (B2)
     */
    val x_b1_b2_y = (x_b1_b2
      .join(b1_b2_y,Seq(SparkMatroid.colBfId,"b2"))
      /* now we have (x,b1,b2,y) for all _candidates_ y*/
        .join(b1_x_y.withColumn("exists",lit(1)),
          Seq(SparkMatroid.colBfId,"x","y"),"left")
      )

    /**
     * this is the family of good triples B1,x,B2:
     */
    val x_b1_b2_good = x_b1_b2_y.select(SparkMatroid.colBfId,"b2","x")

    /**
     * the counter examples data frame
     */

    val counter_ex = x_b1_b2_good.filter(new ColumnName("exists").isNull)

    val wrong = (if (failFast) counter_ex.limit(1) else counter_ex).count

    if (wrong == 0)
      TestResult("[v] axiom (B2) holds.")
    else
      TestResult(s"[x] There are ${if (failFast) "at least " else ""}${wrong} counter-examples to (B2).")
  }
}
