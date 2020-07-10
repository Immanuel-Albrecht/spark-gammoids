package plus.albrecht.matroids.adapters

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType
import plus.albrecht.matroids.traits.SparkMatroid

import scala.reflect.ClassTag

/**
  * Provides an abstract class for implementations of the SparkMatroid interface
  * with lazy data frames
  *
  * @tparam T   matroid element type
  */
abstract class DataFrameSparkMatroid[T: ClassTag]() extends SparkMatroid[T] {

  /** matroid element type */
  lazy val _elementType = SparkMatroid.getSparkType[T]()

  override def elementType(): DataType = _elementType

  /**
    * store bases as indexed sets
    */
  val dfBasisFamily: DataFrame

  /**
    * store ground set
    */
  val dfGroundSet: DataFrame

  /**
    * store the rank
    */
  val dfRank: DataFrame

  override def df(data: String): Option[DataFrame] = data match {
    case x if x == SparkMatroid.dataBasisFamily ⇒ Some(dfBasisFamily)
    case x if x == SparkMatroid.dataGroundSet ⇒ Some(dfGroundSet)
    case x if x == SparkMatroid.dataRank ⇒ Some(dfRank)
    case x ⇒ super.df(data)
  }

}
