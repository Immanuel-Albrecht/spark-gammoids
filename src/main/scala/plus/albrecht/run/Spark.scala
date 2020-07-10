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
    val session = SparkSession
      .builder()
      //.master("local")
      //.appName("plus.albrecht.run.Spark")
      .getOrCreate()

    println(s"Created spark session ${session}.")

    println("Configuration values:")
    session.conf.getAll.foreach({
      case (k, v) ⇒ println(s"  ${k}  = ${v}")
    })

    session
  }
}
