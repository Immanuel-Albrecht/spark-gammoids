import plus.albrecht.run._
import plus.albrecht.matroids._
import plus.albrecht.digraphs._
import plus.albrecht.iterables._
import org.apache.spark.sql.ColumnName
import plus.albrecht.digraphs._
import plus.albrecht.digraphs.spark._
import plus.albrecht.digraphs.traits._
import plus.albrecht.matroids.spark._
import plus.albrecht.matroids.spark.traits._

def getSize(x : AnyRef) = org.apache.spark.util.SizeEstimator.estimate(x)

lazy val not_mk4 = new BasisMatroid[String](Set(
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

lazy val M = SparkBasisMatroid(not_mk4)

lazy val MK4 = SparkBasisMatroid(NamedMatroid("M(K4)"))


