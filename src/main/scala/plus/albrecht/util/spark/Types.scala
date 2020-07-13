package plus.albrecht.util.spark

import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, FloatType, IntegerType, LongType, StringType}
import plus.albrecht.matroids.spark.traits.SparkMatroid

import scala.reflect.{ClassTag, classTag}

/**
 *
 * common object for converting scala and spark types
 *
 */
object Types {

  /**
   *
   * @tparam X class
   *
   * @return DataType corresponding to X
   */
  def getSparkType[X: ClassTag](): DataType = getSparkType(classTag[X].runtimeClass.toString)

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
    case x ⇒
      throw new Exception(
        s"Scala type class ${x} is not supported on spark!"
      )
  }

}
