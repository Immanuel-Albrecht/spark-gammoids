package plus.albrecht.run

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataType

import scala.reflect.ClassTag

/**
  * wrapper object for spark session
  */
object Spark {

  /**
    * single SparkSession object to be used
    *
    * reads the Config() configuration for the tag Set("spark"),
    * if the following values are set, then
    *   "master" -> spark master
    *   "app-name" -> spark application name
    */
  lazy val spark: SparkSession = {

    /**
      * Config object
      */
    val cfg = Config().setTagSet(Set("spark"))

    val masterCfg = cfg.getValueOrDefault("master", null)

    val appNameCfg = cfg.getValueOrDefault("app-name", null)

    val setAppName: SparkSession.Builder ⇒ SparkSession.Builder =
      if (appNameCfg != null)(_.appName(appNameCfg.toString)) else (x ⇒ x)

    val setMaster: SparkSession.Builder ⇒ SparkSession.Builder =
      if (masterCfg != null)(_.master(masterCfg.toString)) else (x ⇒ x)

    val session = setAppName(setMaster(SparkSession.builder())).getOrCreate()

    println(s"Created spark session ${session}.")

    println("Configuration values:")
    session.conf.getAll.foreach({
      case (k, v) ⇒ println(s"  ${k}  = ${v}")
    })

    session
  }

}
