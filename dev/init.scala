import plus.albrecht.run._
import plus.albrecht.matroids._
import plus.albrecht.digraphs._

import plus.albrecht.matroids.traits.SparkMatroid
import org.apache.spark.sql.ColumnName

def getSize(x : AnyRef) = org.apache.spark.util.SizeEstimator.estimate(x)

val not_mk4 = new BasisMatroid[String](Set(
  Set("a", "b", "d"),
  Set("a", "b", "e"),
  Set("a", "b", "f"),
  Set("a", "c", "d"),
  Set("a", "c", "e"),
  Set("a", "c", "f"),
  Set("a", "d", "e"),
  Set("a", "d", "f"),
  Set("b", "c", "d"),
  // Set("b","c","e"),
  Set("b", "c", "f"),
  Set("b", "d", "f"),
  Set("b", "e", "f"),
  Set("c", "d", "e"),
  Set("c", "e", "f"),
  Set("d", "e", "f")))

val M = SparkBasisMatroid(not_mk4)