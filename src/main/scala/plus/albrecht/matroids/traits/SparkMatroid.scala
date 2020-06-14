package plus.albrecht.matroids.traits

import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import plus.albrecht.run.Spark

import scala.reflect.{ClassTag, classTag}

/**
 * base trait for matroids that are stored in spark
 *
 * @tparam T matroid element type (scala class, like Int or String)
 */
trait SparkMatroid[T] {


  /**
   * We use this method to obtain the correct StructTypes for different Scala
   * classes.
   *
   * @param name  class name, obtain by classOf[...].toString
   * @return  DataType object corresponding to X
   */
  def getSparkType(name : String) : DataType = name match {
    case x if x == classOf[Int].toString ⇒ IntegerType
    case x if x == classOf[Long].toString ⇒ LongType
    case x if x == classOf[String].toString ⇒ StringType
    case x if x == classOf[Float].toString ⇒ FloatType
    case x if x == classOf[Double].toString ⇒ DoubleType
    case x if x == classOf[Boolean].toString ⇒ BooleanType
    case x ⇒ throw new Exception(s"Matroid element class ${x} is not supported on spark!")
  }

  /**
   *
   * @tparam X  class
   * @return DataType corresponding to X
   */
  def getSparkType[X : ClassTag]() : DataType = getSparkType(classTag[X].runtimeClass.toString)

  /**
   * StructType corresponding to the element class T;
   * we have to do it this way because we cannot have [T:ClassTag] in traits.
   */
  def elementType() : DataType


  /**
   * schema for storing the basisFamily
   */

  lazy val basisFamilySchema = StructType(
    StructField("basis_id", getSparkType[Long](), false)::
      StructField("element", elementType, false) :: Nil)



  /**
   * get the spark session used for this matroid
   *
   * @return spark session
   */
  def spark() : SparkSession = Spark.spark /* probably use the default spark session */



  /**
   *
   * returns a data frame associated with the given requested data
   *
   * @param data   requested data frame
   * @return None if the request is unknown or cannot be fulfilled, the correct DataFrame otherwise.
   */
  def df(data : String) : Option[DataFrame] = None

  /** string for requesting the BasisFamilies data frame */
  val dataBasisFamilies = "basisFamilies"

}
