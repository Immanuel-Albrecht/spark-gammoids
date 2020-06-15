package plus.albrecht.matroids.traits

import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{ColumnName, DataFrame, Row, SparkSession}
import plus.albrecht.matroids.traits.SparkMatroid.getSparkType
import plus.albrecht.run.Spark

import scala.reflect.{ClassTag, classTag}

/**
 * base trait for matroids that are stored in spark
 *
 * @tparam T matroid element type (scala class, like Int or String)
 */
trait SparkMatroid[T] {


  /**
   *
   * @tparam X class
   *
   * @return DataType corresponding to X
   */
  def getSparkType[X: ClassTag](): DataType = SparkMatroid.getSparkType(classTag[X].runtimeClass.toString)

  /**
   * StructType corresponding to the element class T;
   * we have to do it this way because we cannot have [T:ClassTag] in traits.
   */
  def elementType(): DataType

  /**
   * schema for storing the basisFamily
   */

  lazy val basisFamilySchema = StructType(
    StructField(SparkMatroid.colBfId, getSparkType[Long](), false) ::
      StructField(SparkMatroid.colBfElement, elementType, false) :: Nil)

  /**
   * schema for storing the rank
   */

  lazy val rankSchema = StructType(
    StructField(SparkMatroid.colRkRank, getSparkType[Int](), false) :: Nil)



  /**
   * schema for storing the groundSet
   */

  lazy val groundSetSchema = StructType(
    StructField(SparkMatroid.colBfElement, elementType, false) :: Nil)


  /**
   * get the spark session used for this matroid
   *
   * @return spark session
   */
  def spark(): SparkSession = Spark.spark /* probably use the default spark session */

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
      /* try to infer ground set from bases, if there are loops, we are lost here. */
      val dfB = this.df(SparkMatroid.dataBasisFamily)
      if (dfB.isDefined) {
        Some(
          dfB
            .get
            .select(SparkMatroid.colBfElement)
            .withColumnRenamed(SparkMatroid.colBfElement, SparkMatroid.colGsElement)
            .distinct)
      } else None
    case x if x == SparkMatroid.dataRank ⇒
      /* obtain the rank from the first base */
      val dfB = this.df(SparkMatroid.dataBasisFamily)
      if (dfB.isDefined) {
        val df = dfB.get
        val first_id = df.limit(1).select(SparkMatroid.colBfId).collect.map(r=>r.get(0)).head
        val rdd = spark().sparkContext.parallelize(
          Seq(Row(df.filter(new ColumnName(SparkMatroid.colBfId).isin(first_id))
            .distinct.count.intValue())))
        Some(
          spark().createDataFrame(rdd, rankSchema)
        )
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
   * We use this method to obtain the correct StructTypes for different Scala
   * classes.
   *
   * @param name class name, obtain by classOf[...].toString
   *
   * @return DataType object corresponding to X
   */
  def getSparkType(name: String): DataType = name match {
    case x if x == classOf[Int].toString ⇒ IntegerType
    case x if x == classOf[Long].toString ⇒ LongType
    case x if x == classOf[String].toString ⇒ StringType
    case x if x == classOf[Float].toString ⇒ FloatType
    case x if x == classOf[Double].toString ⇒ DoubleType
    case x if x == classOf[Boolean].toString ⇒ BooleanType
    case x ⇒ throw new Exception(s"Matroid element class ${x} is not supported on spark!")
  }

}
