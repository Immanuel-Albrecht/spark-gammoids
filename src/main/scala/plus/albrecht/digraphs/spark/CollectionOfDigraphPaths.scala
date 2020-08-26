package plus.albrecht.digraphs.spark

import org.apache.spark.sql.{ColumnName, DataFrame}

/**
  * class that encapsulates the usage of pre-computed families of digraph paths
  * and massively computing gammoids
  *
  * @param df_allPaths  data frame with contents like in DigraphFamily.df_allPaths
  * @tparam V
  */
class CollectionOfDigraphPaths[V](val df_allPaths: DataFrame) {

  lazy val df_gammoids: DataFrame = {
    throw new Exception("TODO!")

  }

}

/**
  * companion object used to create CollectionOfDigraphPaths objects
  */
object CollectionOfDigraphPaths {

  /** name of digraph id column */
  val id: String = DigraphFamily.id

  /** name of the path column */
  val path: String = DigraphFamily.path

  /** name of the number of elements column */
  val nbrElements: String = "ELEMENTS"

  /** name of the rank column */
  val rank: String = "RANK"

  /** name of the basis indicator string column */
  val bases: String = "BASES"

  /** name of the target vertex set column */
  val targets: String = "TARGETS"

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

}
