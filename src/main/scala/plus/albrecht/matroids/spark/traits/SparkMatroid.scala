package plus.albrecht.matroids.spark.traits

import org.apache.spark.sql.types._
import org.apache.spark.sql.{ColumnName, DataFrame, Row, SparkSession}
import plus.albrecht.run.Spark
import plus.albrecht.util.spark.Types

import scala.reflect.{ClassTag, classTag}

/**
 * base trait for matroids that are stored in spark
 *
 * @tparam T matroid element type (scala class, like Int or String)
 */
trait SparkMatroid[T] {

  /**
   * StructType corresponding to the element class T;
   * we have to do it this way because we cannot have [T:ClassTag] in traits.
   */
  def elementType(): DataType

  /**
   * schema for storing the basisFamily
   */
  lazy val basisFamilySchema = StructType(
    StructField(SparkMatroid.colBfId, Types.getSparkType[Long](), false) ::
      StructField(SparkMatroid.colBfElement, elementType, false) :: Nil
  )

  /**
   * schema for storing the rank
   */
  lazy val rankSchema = SparkMatroid.rankSchema

  /**
   * schema for storing the groundSet
   */
  lazy val groundSetSchema = StructType(
    StructField(SparkMatroid.colBfElement, elementType, false) :: Nil
  )

  /**
   * get the spark session used for this matroid
   *
   * @return spark session
   */
  def spark(): SparkSession =
    Spark.spark /* probably use the default spark session */

  /**
   *
   * returns a data frame associated with the given requested data
   *
   * @param data requested data frame
   *
   * @return None if the request is unknown or cannot be fulfilled, the correct DataFrame otherwise.
   */
  def df(data: String): Option[DataFrame] = data match {
    case x if x == SparkMatroid.dataGroundSet ⇒
      val dfB = this.df(SparkMatroid.dataBasisFamily)
      if (dfB.isDefined) {
        Some(SparkMatroid.inferGroundSetFromBasisFamily(dfB.get))
      } else None
    case x if x == SparkMatroid.dataRank ⇒
      val dfB = this.df(SparkMatroid.dataBasisFamily)
      if (dfB.isDefined) {
        Some(SparkMatroid.inferRankFromBasisFamily(dfB.get, spark()))
      } else None

    case _ ⇒ None
  }

}

/** companion object */
object SparkMatroid {

  /** names for ground set columns */
  val colGsElement = "element"

  /** names for rank column */
  val colRkRank = "rank"

  /** names for basis family columns */
  val colBfId = "basis_id"
  val colBfElement = "element"

  /** string for requesting the BasisFamily data frame */
  val dataBasisFamily = "basisFamily"

  /** string for requesting the ground set data frame */
  val dataGroundSet = "groundSet"

  /** string for requesting the rank data frame */
  val dataRank = "rank"


  /**
   *
   * Try to infer ground set from bases, if (and only if) there are loops, we are lost here.
   *
   * @param dfB
   *
   * @return
   */
  def inferGroundSetFromBasisFamily(dfB: DataFrame): DataFrame = {
    dfB
      .select(SparkMatroid.colBfElement)
      .withColumnRenamed(SparkMatroid.colBfElement, SparkMatroid.colGsElement)
      .distinct
      .cache()
  }

  /**
   * count the number of elements of any basis to obtain the rank
   *
   * @param dfB basis dataframe
   *
   * @return
   */
  def inferRankFromBasisFamily(dfB: DataFrame, spark: SparkSession) = {

    val first_id = dfB
      .limit(1)
      .select(SparkMatroid.colBfId)
      .collect
      .map(r => r.get(0))
      .head
    val rdd = spark.sparkContext.parallelize(
      Seq(
        Row(
          dfB
            .filter(new ColumnName(SparkMatroid.colBfId).isin(first_id))
            .distinct
            .count
            .intValue()
        )
      )
    )
    spark.createDataFrame(rdd, rankSchema).cache()
  }



  /**
   * the rank schema is the same for every matroid regardless of element type
   */
  lazy val rankSchema = StructType(
    StructField(SparkMatroid.colRkRank, Types.getSparkType[Int](), false) :: Nil
  )
}
