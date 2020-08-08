package plus.albrecht.digraphs.spark

import org.apache.spark.sql.{Column, ColumnName, DataFrame, Row}
import plus.albrecht.run.Spark
import plus.albrecht.util.spark.Types

import scala.reflect.ClassTag
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{
  ArrayType,
  DataType,
  IntegerType,
  StructField,
  StructType
}
import plus.albrecht.digraphs.Digraph

/**
  * class for accessing digraph families stored in spark data frames.
  * Note: _This implementation disregards isolated vertices!_
  *
  * @param df  digraph family dataframe
  *            This dataframe _must_ have the following columns:
  *            DigraphFamily.id, DigraphFamily.u, DigraphFamily.v
  *            which describe the arcs in the respective digraphs.
  *
  *            Additional columns are ignored.
  *
  * @tparam V vertex type
  */
class DigraphFamily[V: ClassTag](val df: DataFrame) {

  def elementType(): DataType = Types.getSparkType[V]()

  /**
    *  data frame where each row corresponds to a single digraph of the family
    *  determined by its arc set
    */
  lazy val df_arcs: DataFrame = df
    .groupBy("ID")
    .agg(
      collect_set(struct(DigraphFamily.colU, DigraphFamily.colV))
        .as(DigraphFamily.arcs)
    )

  /**
    * data frame that contains all paths of each
    * digraph in the family
    */
  lazy val df_allPaths: DataFrame = {
    val schema = ArrayType(ArrayType(elementType()))

    val generate_paths = udf(
      (arcs: Seq[Row]) ⇒ {
        Digraph[V](arcs.map(x ⇒ (x.getAs[V](0), x.getAs[V](1))).toList).allPaths
          .map(_.toArray[Any])
          .toArray[Any]
      },
      schema
    )

    val elt_schema = elementType()

    val first_element = udf(
      (path: Seq[Any]) ⇒ {
        path.head
      },
      elt_schema
    )

    val last_element = udf(
      (path: Seq[Any]) ⇒ {
        path.last
      },
      elt_schema
    )

    df_arcs
      .select(
        DigraphFamily.colId,
        explode(generate_paths(DigraphFamily.colArcs)).as(DigraphFamily.path)
      )
      .select(
        DigraphFamily.colId,
        first_element(DigraphFamily.colPath).as(DigraphFamily.u),
        last_element(DigraphFamily.colPath).as(DigraphFamily.v),
        DigraphFamily.colPath
      )
      .repartition(DigraphFamily.colId, DigraphFamily.colU, DigraphFamily.colV)
      .cache()
  }

}

/**
  * companion object used to create DigraphFamily objects
  */
object DigraphFamily {

  /**
    * spark session object
    */
  lazy val spark = Spark.spark

  /** name of digraph id column */
  val id: String = "ID"

  /** name of digraph arc source column */
  val u: String = "U"

  /** nome of digraph arc target column */
  val v: String = "V"

  /** name of the arc set  [(u,v),..] column */
  val arcs: String = "ARCS"

  /** name of the path column */
  val path: String = "PATH"

  /** digraph id column object */
  val colId = new ColumnName(id)

  /** digraph arc source column object */
  val colU = new ColumnName(u)

  /** digraph arc target column object */
  val colV = new ColumnName(v)

  /** digraph path column object */
  val colPath = new ColumnName(path)

  /** digraph arc set column object */
  val colArcs = new ColumnName(arcs)

  /**
    * Puts a sequence of digraphs as family in a spark data frame and
    * creates the corresponding DigraphFamily object.
    *
    * @param digraphs  sequence of digraphs
    * @param id0       id for digraphs.head
    * @param next_id   generate next id
    * @tparam V        vertex type
    * @tparam ID       id type
    * @return DigraphFamilyObject
    */
  def apply[V: ClassTag, ID: ClassTag](
      digraphs: Seq[Digraph[V]],
      id0: ID,
      next_id: ID ⇒ ID
  ): DigraphFamily[V] = {

    val arcsSeq = digraphs
      .foldLeft((Seq[Row](), id0))({
        case ((s0, id), d) ⇒ {
          (
            s0 ++ d.iterateArcs().map({ case (u: V, v: V) ⇒ Row(id, u, v) }),
            next_id(id)
          )
        }
      })
      ._1

    val rdd = spark.sparkContext.parallelize(arcsSeq)

    val schema = StructType(
      StructField(id, Types.getSparkType[ID](), false) ::
        StructField(u, Types.getSparkType[V](), false) ::
        StructField(v, Types.getSparkType[V](), false) :: Nil
    )

    val df = spark.createDataFrame(rdd, schema).cache()

    new DigraphFamily[V](df)

  }

  /**
    * Puts a sequence of digraphs as family in a spark data frame and
    * creates the corresponding DigraphFamily object.
    *
    * Uses the sequence index as id.
    *
    * @param digraphs  sequence of digraphs
    * @tparam V        vertex type
    * @tparam ID       id type
    * @return DigraphFamilyObject
    */
  def apply[V: ClassTag, ID: ClassTag](
      digraphs: Seq[Digraph[V]]
  ): DigraphFamily[V] = {
    apply[V, Int](digraphs, 0, (x: Int) ⇒ x + 1)
  }

  /**
    * Loads a digraph family from the given source,
    * allows for renaming and type-casting the three columns for id, u, and v.
    *
    * @param path
    * @param format
    * @param idName
    * @param uName
    * @param vName
    * @param filter   a filter condition on the digraph family (after renaming)
    * @tparam V
    * @tparam ID
    * @return DigraphFamily object
    */
  def apply[V: ClassTag, ID: ClassTag](
      path: String,
      format: String,
      idName: String,
      uName: String,
      vName: String,
      filter: Column
  ): DigraphFamily[V] = {
    new DigraphFamily[V](
      spark.read
        .format(format)
        .load(path)
        .select(
          new ColumnName(idName).as(id).cast(Types.getSparkType[ID]()),
          new ColumnName(uName).as(u).cast(Types.getSparkType[V]()),
          new ColumnName(vName).as(v).cast(Types.getSparkType[V]())
        )
        .filter(filter)
    )
  }

  /**
    * * Loads a digraph family from the given source, uses standard column names.
    *
    * @param path
    * @param format
    * @param filter   a filter condition on the digraph family
    * @tparam V
    * @tparam ID
    * @return
    */
  def apply[V: ClassTag, ID: ClassTag](
      path: String,
      format: String,
      filter: Column
  ): DigraphFamily[V] = {
    apply[V, ID](path, format, id, u, v, filter)
  }

}
