package plus.albrecht.digraphs

import plus.albrecht.tests.TestResult

import scala.collection.immutable

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

  /**
   * the inverse incidence as sets
   */

  lazy val invIncidenceSets = invIncidence.mapValues(_.toSet)


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

    lazy val incident_vertices: Set[V] = incidence.flatMap({
      case (u, vs) ⇒
        vs
    }).toSet ++
      incidence.keySet ++ invIncidence.flatMap({
      case (u, vs) ⇒
        vs
    }).toSet ++ invIncidence.keySet


    lazy val arcs_by_incidence: Set[(V, V)] = incidence.flatMap({
      case (u, vs) ⇒
        vs.map({
          case v ⇒ (u, v)
        })
    }).toSet

    lazy val arcs_by_invIncidence: Set[(V, V)] = invIncidence.flatMap({
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
   * Determine all paths (and accompanying PathStats) in the digraph that end in certain targets.
   *
   * @param targets   a set of vertices in which the paths are allowed to end
   * @return  family of (path, PathStat) pairs
   */
  def allPathsAndPathStatsThatEndIn(targets : Iterable[V]) :
  Set[(List[V],PathStats[V])] = {
    val trivial : Set[(List[V],PathStats[V])] =
      targets.map(v ⇒ (List[V](v),PathStats[V](v))).toSet

    (2 to vertices.size) /* maximum number of arcs in a path is #vertices - 1 */
      .foldLeft((trivial,trivial))(
        {
          case ((all_p,new_p),_) ⇒ {
            val longer_paths = new_p.flatMap( {
              case (path,pathstats) ⇒ {
                invIncidence.getOrElse(pathstats.source, Set()).flatMap( v0 ⇒ {
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
   * all paths and their respective PathStats
   */
  lazy val allPathsAndPathStats : Set[(List[V],PathStats[V])] = allPathsAndPathStatsThatEndIn(vertices)

  /**
   * all paths in the digraph
   */
  lazy val allPaths : Set[List[V]] = allPathsAndPathStats.map(_._1).toSet

  /**
   * the minimal statistics of all paths in the digraph
   */
  lazy val allPathStats : Set[PathStats[V]] = allPathsAndPathStats.map(_._2).toSet

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