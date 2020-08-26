package plus.albrecht.digraphs.spark

import org.apache.spark.sql.{Column, ColumnName, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{
  ArrayType,
  IntegerType,
  LongType,
  StringType,
  StructType
}
import plus.albrecht.digraphs.CollectionOfPaths
import plus.albrecht.matroids.Gammoid
import plus.albrecht.run.Spark
import plus.albrecht.util.spark.Types

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * class that encapsulates the usage of pre-computed families of digraph paths
  * and massively computing strict gammoids
  *
  * @param df_allPaths  data frame with contents like in DigraphFamily.df_allPaths
  * @tparam V
  */
class CollectionOfDigraphPaths[V: ClassTag](val df_allPaths: DataFrame) {

  /**
    * output schema for the generator of nonempty subsets of vertices
    */
  lazy val genNonemptySubsetsOutputSchema =
    new StructType().add(
      "sets",
      ArrayType(ArrayType(Types.getSparkType[V](), true), true)
    )

  /**
    * generator function for nonempty subsets of vertices
    */
  lazy val genNonemptySubsets =
    udf(
      ((s: Seq[V]) ⇒ {
        val vertices = s.toSet

        Row(vertices.subsets.filter(!_.isEmpty).toSeq.map(_.toSeq))
      }),
      genNonemptySubsetsOutputSchema
    )

  /**
    * output schema for the computations done with gammoids...
    */
  lazy val computeGammoidColumnsOutputSchema =
    new StructType()
      .add(
        "n",
        IntegerType
      )
      .add(
        "rank",
        IntegerType
      )
      .add(
        "indicator",
        StringType
      )
      .add(
        "ordering",
        ArrayType(Types.getSparkType[V](), true)
      )

  /**
    * compute all the columns for the (strict) gammoids
    */
  lazy val computeGammoidColumns = udf(
    (t: Seq[V], ps: Seq[Seq[V]]) ⇒ {
      val targets = t.toSet
      val paths = CollectionOfPaths(ps.map(_.toList).toSet)
      val groundSet = paths.vertices().toSet
      val M = Gammoid[V](paths, targets, groundSet)

      val n = groundSet.size
      val rank = M.rank()
      val indicator = M.basisIndicatorString
      val ordering = M.canonicalOrdering

      Row(n, rank, indicator, ordering)
    },
    computeGammoidColumnsOutputSchema
  )

  /**
    * data frame that computes all the less trivial (strict) gammoids that
    * correspond to a given family of digraph paths.
    */
  lazy val df_gammoids: DataFrame = {
    import CollectionOfDigraphPaths._

    df_allPaths
      .groupBy(colId)
      /* obtain a column with the vertices of the digraph, and a column with the
       paths in the digraph with given identifier.
       */
      .agg(collect_set(colPath).as("paths"), collect_set(colU).as("vertices"))
      /* blow up the vertices row to obtain a row with all non-empty target vertices in a digraph */
      .withColumn("target_options", genNonemptySubsets(col("vertices")))
      .select(
        colId,
        explode(col("target_options.sets")).as(targets),
        col("paths")
      )
      /* at this point, each row corresponds to one representation of a strict gammoid */
      .withColumn("computed", computeGammoidColumns(colTargets, col("paths")))
      .select(
        col("computed.n").as(nbrElements),
        col("computed.rank").as(rank),
        col("computed.indicator").as(bases),
        colId,
        colTargets,
        col("computed.ordering").as(canonicalOrdering)
      )
      .sort(colRank, colId, colBases)
  }

}

/**
  * companion object used to create CollectionOfDigraphPaths objects
  */
object CollectionOfDigraphPaths {

  /**
    * spark session object
    */
  lazy val spark = Spark.spark

  /**
    * column operation that turns a
    */
  lazy val getAllNonemptySubsets = udf({ collection: Seq[Row] ⇒ collection })

  /** name of digraph id column */
  val id: String = DigraphFamily.id

  /** name of the path column */
  val path: String = DigraphFamily.path

  /** name of the source vertex column */
  val u: String = DigraphFamily.u

  /** name of the number of elements column */
  val nbrElements: String = "ELEMENTS"

  /** name of the rank column */
  val rank: String = "RANK"

  /** name of the basis indicator string column */
  val bases: String = "BASES"

  /** name of the target vertex set column */
  val targets: String = "TARGETS"

  /** name of the column that holds the canonical ordering of the string gammoid */
  val canonicalOrdering: String = "CANONICAL_ORDER"

  /** digraph source column object */
  val colU = new ColumnName(u)

  /** digraph id column object */
  val colId = new ColumnName(id)

  /** digraph path column object */
  val colPath = new ColumnName(path)

  /** gammoid number of elements column object */
  val colNbrElements = new ColumnName(nbrElements)

  /** gammoid target vertex set column object */
  val colTargets = new ColumnName(targets)

  /** gammoid rank column object */
  val colRank = new ColumnName(rank)

  /** gammoid basis indicator string column object */
  val colBases = new ColumnName(bases)

  /** gammoid canonical ordering column object */
  val colCanonicalOrdering = new ColumnName(canonicalOrdering)

  /**
    * Loads a digraph family from the given source,
    * allows for renaming and type-casting the three columns for id, u, and v.
    *
    * @param path
    * @param format
    * @param idName
    * @param pathName
    * @param uName
    * @param filter   a filter condition on the digraph family (after renaming)
    * @tparam V
    * @tparam ID
    * @return CollectionOfDigraphPaths object
    */
  def apply[V: ClassTag, ID: ClassTag](
      path: String,
      format: String,
      idName: String,
      pathName: String,
      uName: String,
      filter: Column
  ): CollectionOfDigraphPaths[V] = {
    new CollectionOfDigraphPaths[V](
      spark.read
        .format(format)
        .load(path)
        .select(
          new ColumnName(idName).as(id).cast(Types.getSparkType[ID]()),
          new ColumnName(uName).as(u).cast(Types.getSparkType[V]()),
          new ColumnName(pathName).as(CollectionOfDigraphPaths.path)
        )
        .filter(filter)
    )
  }

  /**
    * Loads a CollectionOfDigraphPaths from given source.
    *
    * @param path
    * @param format
    * @param filter   for instance, col("ID") <== 100
    * @tparam V
    * @tparam ID
    * @return CollectionOfDigraphPaths
    */
  def apply[V: ClassTag, ID: ClassTag](
      path: String,
      format: String,
      filter: Column
  ): CollectionOfDigraphPaths[V] =
    apply[V, ID](path, format, id, CollectionOfDigraphPaths.path, u, filter)

  /**
    *  Loads a CollectionOfDigraphPaths from given source.
    *
    * @param path
    * @param format
    * @tparam V
    * @tparam ID
    * @return
    */

  def apply[V: ClassTag, ID: ClassTag](
      path: String,
      format: String
  ): CollectionOfDigraphPaths[V] =
    apply[V, ID](path, format, lit(true))

  /**
    * Loads a CollectionOfDigraphPaths from a parquet source
    *
    * @param path
    * @tparam V
    * @tparam ID
    * @return
    */
  def apply[V: ClassTag, ID: ClassTag](
      path: String
  ): CollectionOfDigraphPaths[V] =
    apply[V, ID](path, "parquet")

  /**
    * Loads a CollectionOfDigraphPaths from a parquet source
    *
    * @param path
    * @param filter
    * @tparam V
    * @tparam ID
    * @return
    */
  def apply[V: ClassTag, ID: ClassTag](
      path: String,
      filter: Column
  ): CollectionOfDigraphPaths[V] =
    apply[V, ID](path, "parquet", filter)

}
