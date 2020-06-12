package plus.albrecht.digraphs

import plus.albrecht.tests.TestResult

/**
 * Object that stores some information of a path (or walk) in a digraph.
 *
 * The information stored is what might be considered the 'minimal statistics'
 * equivalent of a path in a digraph with respect to determining whether a
 * bunch of paths can be combined into a routing.
 *
 * @param source  source vertex of the path
 *
 * @param visited all vertices visited by the path, must contain source and target
 *
 * @param target  target vertex of the path
 *
 * @tparam V vertex type in the digraph
 */
case class QuasiPath[V](source: V, visited: Set[V], target: V) {

  /**
   * is this a valid QuasiPaths object?
   */
  lazy val isValid: TestResult = {
    (
      if (visited contains source) TestResult("[v] Source is visited.")
      else TestResult("[x] Source is not visited.")) ++ (
      if (visited contains target) TestResult("[v] Target is visited.")
      else TestResult("[x] Target is not visited.")
      )
  }

  /**
   * adds a new source to the path (unless v is already visited,
   * in that case, returns this instead)
   *
   * @param v vertex for the path (v,[..this path])
   *
   * @return altered path stats
   */
  def addSource(v: V): QuasiPath[V] = {
    if (visited contains v) this else
      new QuasiPath(v, visited ++ Set(v), target)
  }

}

/**
 * companion for easy construction
 */
object QuasiPath {

  /**
   * creates stats of a trivial path
   *
   * @param v vertex
   *
   * @tparam V vertex type
   *
   * @return QuasiPaths corresponding to (v)
   */
  def apply[V](v: V): QuasiPath[V] = {
    new QuasiPath[V](v, Set(v), v)
  }

  /**
   * creates stats of a path given as list
   *
   * @param vtx_list a list of vertices traversed
   *
   * @tparam V vertex type
   *
   * @return QuasiPaths object
   */
  def apply[V](vtx_list: Iterable[V]): QuasiPath[V] = {
    new QuasiPath[V](vtx_list.head, vtx_list.toSet, vtx_list.last)
  }
}