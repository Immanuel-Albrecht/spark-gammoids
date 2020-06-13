package plus.albrecht.digraphs

import plus.albrecht.tests.TestResult
import plus.albrecht.util.Lazy

import scala.collection.immutable

/**
 * object representing a directed graph (digraph).
 *
 * @param _vertices     Vertices in the digraph
 *
 * @param _incidence    Family of incidence lists: if y \in incidence[x],
 *                      then there is a n arc y->x in the digraph
 *
 * @param _invIncidence Family of inverse incidence lists: if x \in invIncidence[y],
 *                      then there is an arc x->y in the digraph
 *
 * @tparam V vertex type
 */
class Digraph[V](val _vertices: Iterable[V],
                 val _incidence: Map[V, Iterable[V]],
                 val _invIncidence: Map[V, Iterable[V]]) extends traits.PathStructure[V] {

  /**
   * cache the dual digraph, this way, we achieve this.opp().opp() === this,
   * we initialize it once it is requested
   */
  private var reflectDual: Digraph[V] = null

  /**
   * get the dual digraph
   * (i.e. the Digraph with all arcs reversed)
   *
   * @return Digraph object
   */
  def opp(): Digraph[V] = {
    if (reflectDual == null) {
      reflectDual = new Digraph[V](_vertices, _invIncidence, _incidence)
      reflectDual.reflectDual = this
    }
    reflectDual
  }


  override def vertices(): Iterable[V] = _vertices


  /**
   * the vertices as set
   */
  lazy val vertexSet = _vertices.toSet


  /**
   * the incidences as sets
   */
  lazy val incidenceSets = _incidence.mapValues(_.toSet)

  /**
   * the inverse incidence as sets
   */

  lazy val invIncidenceSets = _invIncidence.mapValues(_.toSet)

  /**
   * Tests whether the incidence of this digraph equals the given other
   * incidence.
   *
   * @param other_incidence
   *
   * @return true, if both incidences describe the same arc set
   */
  def compareIncidence(other_incidence: Map[V, Set[V]]): Boolean = {

    _incidence.keySet.union(other_incidence.keySet).foldLeft(true)({
      case (all_good, v) ⇒
        if (all_good) {
          val vx0 = incidenceSets.getOrElse(v, Set())
          val vx1 = other_incidence.getOrElse(v, Set()).toSet
          vx0 == vx1
        } else false
    })
  }

  /**
   * determine whether two digraphs are equal,
   * assuming that if both objects are Digraphs,
   * then they are both valid objects.
   *
   * @param that compare to this object
   *
   * @return true, if both digraphs are equal
   */
  override def equals(that: Any): Boolean = that match {
    case that: Digraph[V] ⇒ {
      this.vertexSet == that.vertexSet &&
        compareIncidence(that.incidenceSets)
      /* assuming that both objects are valid, we can skip this._invIncidence == that
      ._invIncidence */
    }
    case _ ⇒ false
  }


  /**
   * Tests whether:
   *   - every arc connects two elements of the vertex set
   *   - incidence and invIncidence are
   *
   * @param failFast stop testing as soon as it is clear that this object is invalid
   *
   * @return test result
   */
  def isValid(failFast: Boolean): TestResult = {

    lazy val incident_vertices: Set[V] = _incidence.flatMap({
      case (u, vs) ⇒
        vs
    }).toSet ++
      _incidence.keySet ++ _invIncidence.flatMap({
      case (u, vs) ⇒
        vs
    }).toSet ++ _invIncidence.keySet


    lazy val arcs_by_incidence: Set[(V, V)] = _incidence.flatMap({
      case (u, vs) ⇒
        vs.map({
          case v ⇒ (u, v)
        })
    }).toSet

    lazy val arcs_by_invIncidence: Set[(V, V)] = _invIncidence.flatMap({
      case (v, us) ⇒
        us.map({
          case u ⇒ (u, v)
        })
    }).toSet

    /* here go the tests */
    val tests: List[() ⇒ TestResult] = List(
      () ⇒ {
        if (incident_vertices subsetOf vertexSet) TestResult(true,
          "[v] Arcs and incidence lists consists of vertices of this digraph." :: Nil)
        else TestResult(false, f"[x] ${
          incident_vertices.diff(vertexSet)
            .size
        } referenced vertices are not in the vertex set of this digraph." :: Nil)
      },
      () ⇒ {
        if (arcs_by_incidence == arcs_by_invIncidence) TestResult(true,
          "[v] incidence and invIncidence structure mirror each other." :: Nil)
        else {
          TestResult(false, f"[x] incidence has ${
            arcs_by_incidence.diff(arcs_by_invIncidence)
              .size
          } extra arcs, invIncidence has ${
            arcs_by_invIncidence.diff(arcs_by_incidence)
              .size
          } extra arcs" :: Nil)
        }
      },
    )

    /* apply the tests one after another */
    tests.foldLeft(TestResult(true, List()))({
      case (last_result, next_test) ⇒ {
        if (failFast && (!last_result.passed)) last_result
        else last_result ++ next_test()
      }
    })

  }

  /**
   * quick check whether the digraph's values are consistent
   *
   * @return isValid(true)
   */
  def isValid(): TestResult = isValid(true)


  /**
   * Determine all paths (and accompanying QuasiPaths) in the digraph that end in certain
   * targets.
   *
   * @param targets a set of vertices in which the paths are allowed to end
   *
   * @return family of (path, QuasiPath) pairs
   */
  def allPathsAndPathStatsThatEndIn(targets: Iterable[V]):
  Set[(List[V], QuasiPath[V])] = {
    val trivial: Set[(List[V], QuasiPath[V])] =
      targets.map(v ⇒ (List[V](v), QuasiPath[V](v))).toSet

    (2 to _vertices.size) /* maximum number of arcs in a path is #vertices - 1 */
      .foldLeft((trivial, trivial))(
        {
          case ((all_p, new_p), _) ⇒ {
            val longer_paths = new_p.flatMap({
              case (path, pathstats) ⇒ {
                _invIncidence.getOrElse(pathstats.source, Set()).flatMap(v0 ⇒ {
                  if (pathstats.visited contains v0) Nil
                  else
                    (List(v0) ++ path, pathstats.addSource(v0)) :: Nil
                })
              }
            })

            (all_p ++ longer_paths, longer_paths)
          }
        }
      )._1
  }

  /**
   * all paths and their respective QuasiPaths
   */
  lazy val allPathsAndPathStats: Set[(List[V], QuasiPath[V])] =
    allPathsAndPathStatsThatEndIn(_vertices)

  /**
   * all paths in the digraph
   */
  lazy val allPaths: Set[List[V]] = allPathsAndPathStats.map(_._1).toSet

  /**
   * the minimal statistics of all paths in the digraph
   */
  lazy val allPathStats: Set[QuasiPath[V]] = allPathsAndPathStats.map(_._2).toSet


  /**
   * super lazy path stats
   */

  lazy val pathStatsEndingInStartingIn:
    Map[V, Lazy[Map[V, Set[QuasiPath[V]]]]] = {
    _vertices.map(v ⇒ {
      v → Lazy({
        /* determine paths ending in v */
        val paths_to_v = allPathsAndPathStatsThatEndIn(Set(v)).map(_._2)
        paths_to_v.foldLeft(Map[V, Set[QuasiPath[V]]]())(
          {
            case (m, p) ⇒ m ++ Map(p.source →
              (m.getOrElse(p.source, Set[QuasiPath[V]]()) ++ Set(p)))
          }
        )
      }
      )
    }).toMap
  }

  override def paths(sources: Iterable[V],
                     avoiding: Iterable[V],
                     targets: Iterable[V]): Iterable[QuasiPath[V]] = {
    val avoid: Set[V] = avoiding.toSet
    val t: Set[V] = targets.toSet.diff(avoid)
    val s: Set[V] = sources.toSet.diff(avoid)


    t.foldLeft(Set[QuasiPath[V]]())({
      case (paths, target) ⇒ {
        s.foldLeft(paths)({
          case (paths, source) ⇒
            paths ++ pathStatsEndingInStartingIn.getOrElse(target,
              Lazy(Map[V, Set[QuasiPath[V]]]()))().getOrElse(
              source, Set[QuasiPath[V]]()).filter(
              ps ⇒ avoid.intersect(ps.visited).isEmpty).toSet
        })
      }
    })
  }

}


/**
 * companion object that helps creating valid digraphs
 */
object Digraph {


  /**
   * creates a digraph induced by a given set of arcs
   *
   * @param arcs a list of arcs
   *
   * @tparam V vertex ype
   *
   * @return new Digraph object
   */
  def apply[V](arcs: Iterable[(V, V)]): Digraph[V] = {
    val vertices_inc_inv = arcs.foldLeft(
      (Set[V](),
        Map[V, Set[V]](),
        Map[V, Set[V]]()))({
      case (((vs, inc, inv), (u, v))) ⇒ (
        /* add to vertex set */
        vs ++ Set(u, v),
        /* add to incidence list */
        inc ++ Map(u → (inc.getOrElse(u, Set[V]()) ++ Set(v))),
        /* add to inverse incidence list */
        inv ++ Map(v → (inv.getOrElse(v, Set[V]()) ++ Set(u))),
      )
    })
    val (vertices, incidence, invIncidence) = vertices_inc_inv

    new Digraph[V](vertices, incidence, invIncidence)
  }
}