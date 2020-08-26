package plus.albrecht.digraphs

import plus.albrecht.digraphs.traits.PathStructure

/**
  * This class allows a family of paths to act as a PathStructure,
  * the usage of this class is sound if and only if the given parameter
  * paths_bySource_byTarget actually corresponds to a family of paths of
  * some digraph.
  *
  * @param paths_bySource_byTarget
  *             stores the paths of a digraph, indexed by source and target vertices.
  *             each path is represented as a list of its traversed vertices.
  *
  * @tparam V  vertex type
  */
class CollectionOfPaths[V](
    val paths_bySource_byTarget: Map[V, Map[V, Set[List[V]]]],
    val vertexSet: Set[V]
) extends PathStructure[V] {

  override def vertices(): Iterable[V] = vertexSet.toIterable

  override def paths(
      sources: Iterable[V],
      avoiding: Iterable[V],
      targets: Iterable[V]
  ): Iterable[QuasiPath[V]] = {

    val avoidSet = avoiding.toSet

    sources
      .map(paths_bySource_byTarget.getOrElse(_, Map[V, Set[List[V]]]()))
      .flatMap(atSrc ⇒ targets.map(atSrc.getOrElse(_, Set[List[V]]())))
      .flatMap(_.filter(_.toSet.intersect(avoidSet).isEmpty))
      .map(QuasiPath[V](_))

  }

}

/**
  * companion object
  */
object CollectionOfPaths {

  /**
    * construct a CollectionOfPaths from ..
    * @param paths .. a family of paths stored as list of traversed vertices.
    * @tparam V
    * @return corresponding CollectionOfPaths
    */
  def apply[V](paths: Set[List[V]]): CollectionOfPaths[V] = {

    val vertices = paths.flatten.toSet

    val byTarget = vertices.map(v ⇒ (v, paths.filter(_.last == v))).toMap

    val paths_bySource_byTargets =
      vertices.map(v ⇒ (v, byTarget.mapValues(_.filter(_.head == v)))).toMap

    new CollectionOfPaths[V](paths_bySource_byTargets, vertices)

  }

}
