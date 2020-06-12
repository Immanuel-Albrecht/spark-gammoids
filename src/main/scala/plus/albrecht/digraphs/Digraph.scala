package plus.albrecht.digraphs

/**
 * object representing a directed graph (digraph).
 *
 * @param vertices     Vertices in the digraph
 *
 * @param incidence    Family of incidence lists: if y \in incidence[x],
 *                     then there is a n arc y->x in the digraph
 *
 * @param invIncidence Family of inverse incidence lists: if x \in invIncidence[y],
 *                     then there is an arc x->y in the digraph
 *
 * @tparam V vertex type
 */
class Digraph[V](val vertices: Iterable[V],
                 val incidence: Map[V, Iterable[V]],
                 val invIncidence: Map[V, Iterable[V]]) {
  /**
   * the vertices as set
   */
  lazy val vertexSet = vertices.toSet

  /**
   * the incidences as sets
   */
  lazy val incidenceSets = incidence.mapValues(_.toSet)


  lazy val invIncidenceSets = invIncidence.mapValues(_.toSet)
}
