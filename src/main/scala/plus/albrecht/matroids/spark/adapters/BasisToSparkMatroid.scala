package plus.albrecht.matroids.spark.adapters

import org.apache.spark.sql.{DataFrame, Row}
import plus.albrecht.matroids.traits

import scala.reflect.ClassTag

/**
 * Adapter class that moves a BasisMatroid into the spark cluster.
 *
 * @tparam T matroid element type
 */
class BasisToSparkMatroid[T: ClassTag](val basisMatroid: traits.BasisMatroid[T])
  extends DataFrameSparkMatroid[T] {


  /**
   * store bases as indexed sets
   */
  lazy val dfBasisFamily: DataFrame = {
    val dataSeq = basisMatroid.basisFamily().foldLeft(0L, Seq[Row]())({
      case ((id, seq), b) ⇒
        (id + 1, seq ++ b.map(Row(id, _)).toSeq)
    })._2

    val rdd = spark().sparkContext.parallelize(dataSeq)
    /* TODO: this would be a place where we could add some kind of partitioning
             control
    */

    spark().createDataFrame(rdd,
      basisFamilySchema).cache()
  }

  /**
   * store ground set
   */

  lazy val dfGroundSet: DataFrame = {
    val dataSeq = basisMatroid.groundSetAsSet.map(Row(_)).toSeq

    val rdd = spark().sparkContext.parallelize(dataSeq)
    /* TODO: this would be a place where we could add some kind of partitioning
           control
    */

    spark().createDataFrame(rdd, groundSetSchema).cache()
  }

  /**
   * store the rank
   */
  lazy val dfRank: DataFrame = {
    val dataSeq = Seq(Row(basisMatroid.rank()))
    val rdd = spark().sparkContext.parallelize(dataSeq)
    spark().createDataFrame(rdd, rankSchema).cache()
  }


}

/** companion object */
object BasisToSparkMatroid {

  /**
   * Creates a SparkMatroid from a BasisMatroid
   *
   * @param basisMatroid matroid
   *
   * @tparam T element type
   *
   * @return
   */
  def apply[T: ClassTag](basisMatroid: traits.BasisMatroid[T]) = new BasisToSparkMatroid[T](basisMatroid)
}