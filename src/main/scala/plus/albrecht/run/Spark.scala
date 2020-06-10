package plus.albrecht.run


import org.apache.spark.sql.SparkSession

/**
 * wrapper object for spark session
 */
object Spark {
  /**
   * single SparkSession object to be used
   */
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("plus.albrecht.run.Spark")
      .getOrCreate()
  }
}
