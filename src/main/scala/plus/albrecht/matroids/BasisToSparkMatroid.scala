package plus.albrecht.matroids
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

import scala.reflect.ClassTag

/**
* Adapter class that moves a BasisMatroid into the spark cluster.
*
* @tparam T matroid element type
*/
class BasisToSparkMatroid[T:ClassTag](val basisMatroid: traits.BasisMatroid[T])
  extends traits.SparkMatroid[T] {

  /** matroid element type */
  lazy val _elementType = getSparkType[T]()

  override def elementType() : DataType = _elementType

  lazy val dfBasisFamily : DataFrame = {
    val dataSeq = basisMatroid.basisFamily().foldLeft(0L,Seq[Row]())({
      case ((id, seq), b) ⇒
        (id+1,seq ++ b.map(Row(id, _)).toSeq)
    })._2

    val rdd = spark().sparkContext.parallelize(dataSeq)
    /* TODO: this would be a place where we could add some kind of partitioning
             control
    */

    spark().createDataFrame(rdd,
      basisFamilySchema)
  }

  override def df(data: String): Option[DataFrame] = data match {
    case x if x == dataBasisFamilies ⇒ Some(dfBasisFamily)
    case _ ⇒ None
  }
}

/** companion object */
object BasisToSparkMatroid {

  /**
   * Creates a SparkMatroid from a BasisMatroid
   *
   * @param basisMatroid  matroid
   * @tparam T   element type
   * @return
   */
  def apply[T : ClassTag](basisMatroid: traits.BasisMatroid[T]) = new BasisToSparkMatroid[T](basisMatroid)
}