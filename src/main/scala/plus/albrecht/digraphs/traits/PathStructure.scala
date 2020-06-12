package plus.albrecht.digraphs.traits

import plus.albrecht.digraphs.PathStats

/**
 *
 * interface for querying the existence of paths with certain properties in
 * digraphs
 *
 * @tparam V  vertex type
 */
trait PathStructure[V] {

  /**
   * get the stats of those paths in a digraph that meet the conditions
   *
   * @param sources     set of allowed sources
   * @param avoiding    set of forbidden visits
   * @param targets     set of allowed targets
   * @return  family of PathStats
   */
  def paths(sources : Iterable[V],
            avoiding : Iterable[V],
            targets : Iterable[V]) : Iterable[PathStats[V]]

  /**
   * get the vertex set of the digraph underlying this PathStructure
   *
   * @return  vertices in digraph
   */
  def vertices() : Iterable[V]



}
