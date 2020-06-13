package plus.albrecht.digraphs

import plus.albrecht.tests.TestResult


/**
 * Object that stores some information of a routing in a digraph.
 *
 * The information stored is what might be considered the 'minimal statistics'
 * equivalent of a routing in a digraph with respect to determining whether a
 * bunch of paths can be combined into a routing.
 *
 * @param sources  source vertices of the routing
 *
 * @param visited all vertices visited by any path in the routing, must contain source and target
 *
 * @param targets  target vertices of the routing
 *
 * @tparam V vertex type in the digraph
 */

case class QuasiRouting[V](sources : Set[V], visited : Set[V], targets : Set[V]) {
  /**
   * is this a valid QuasiRouting object?
   */
  lazy val isValid: TestResult = {
    (
      if (sources subsetOf visited) TestResult("[v] Sources are visited.")
      else TestResult(s"[x] ${sources.diff(visited).size} sources are not visited.")) ++ (
      if (targets subsetOf visited) TestResult("[v] Targets are visited.")
      else TestResult(s"[x] ${targets.diff(visited).size} targets are not visited.")
      ) ++ (if (sources.size == targets.size) TestResult("[v] For each source, there is a target.")
    else TestResult(s"[x] There are ${sources.size} sources, but ${targets.size} targets."))
  }

  /**
   * check whether this routing may be augmented by another routing
   * @param quasiRouting  another routing
   * @return true if it may be combined
   */
  def canCombine(quasiRouting: QuasiRouting[V]) : Boolean = {
    this.visited.intersect(quasiRouting.visited).isEmpty
  }

  /**
   * check whether this routing may be augmented by another path
   * @param quasiPath
   * @return true if it may be combined
   */
  def canCombine(quasiPath: QuasiPath[V]) : Boolean = {
    this.visited.intersect(quasiPath.visited).isEmpty
  }

  /**
   * (blindly) combine two routings
   * @param quasiRouting
   * @return combined QuasiRouting
   */
  def ++(quasiRouting: QuasiRouting[V]) : QuasiRouting[V] = {
    new QuasiRouting[V](sources.union(quasiRouting.sources),
      visited.union(quasiRouting.visited),
      targets.union(quasiRouting.targets))
  }

  /**
   * (blindly) combine this routing with a path
   * @param quasiPath
   * @return combined QuasiRouting
   */
  def ++(quasiPath: QuasiPath[V]) : QuasiRouting[V] = {
    new QuasiRouting[V](sources.union(Set(quasiPath.source)),
      visited.union(quasiPath.visited),
      targets.union(Set(quasiPath.target)))
  }

}

object QuasiRouting {

  /**
   * returns the empty QuasiRouting on V
   *
   * @tparam V   vertex type
   * @return
   */
  def apply[V]() = new QuasiRouting[V](Set(),Set(),Set())

}