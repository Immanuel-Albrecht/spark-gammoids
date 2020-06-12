package plus.albrecht.digraphs

/**
 * Object that stores some information of a path (or walk) in a digraph.
 *
 * The information stored is what might be considered the 'minimal statistics'
 * equivalent of a path in a digraph with respect to determining whether a
 * bunch of paths can be combined into a routing.
 *
 * @param source    source vertex of the path
 * @param visited   all vertices visited by the path, must contain source and target
 * @param target    target vertex of the path
 * @tparam V    vertex type in the digraph
 */
case class PathStats[V](source : V, visited : Set[V], target : V) {

  /**
   * is this a valid PathStats object?
   */
  lazy val isValid : Boolean = {
    (visited contains source) && (visited contains target)
  }
}
