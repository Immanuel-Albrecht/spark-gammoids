package plus.albrecht.matroids

import plus.albrecht.digraphs.QuasiRouting
import plus.albrecht.digraphs.traits.PathStructure

/**
 * (companion) object that provides gammoid functionalities
 */
object Gammoid {

  /**
   * creates a BasisMatroid based on the gammoid representation (d,t,e)
   *
   * @param d digraph (we only need its PathStructure though)
   *
   * @param t set of targets of the gammoid (this is actually an iterable,
   *          some orderings may be a lot faster than others)
   *
   * @param e set of edges of the gammoid
   *
   * @tparam V type for vertices/matroid elements
   *
   * @return the gammoid as BasisMatroid
   */
  def apply[V](
                d: PathStructure[V],
                t: Iterable[V],
                e: Set[V]): BasisMatroid[V] = {
    obtainGammoidFromRepresentationAvoiding[V](d, t, e, Set[V]())
  }

  /**
   * creates a BasisMatroid based on the gammoid representation (d',t,e)
   *
   * where d' arises from d by deleting all vertices from 'avoid'
   *
   * @param d     digraph (we only need its PathStructure though)
   *
   * @param t     set of targets of the gammoid (this is actually an iterable,
   *              some orderings may be a lot faster than others)
   *
   * @param e     set of edges of the gammoid
   *
   * @param avoid set of vertices in d that are forbidden to use
   *
   * @tparam V type for vertices/matroid elements
   *
   * @return the gammoid as BasisMatroid
   */
  def obtainGammoidFromRepresentationAvoiding[V](
                                                  d: PathStructure[V],
                                                  t: Iterable[V],
                                                  e: Set[V],
                                                  avoid: Set[V]): BasisMatroid[V] = {
    val (rank, quasi_routings): (Int, Set[QuasiRouting[V]]) =
      (t.foldLeft((0, Set(QuasiRouting[V]()))) /* initialize with the empty routing */
      ({
        case ((rank, quasi_routings), t0) ⇒ {
          val augmented_routings = quasi_routings.flatMap(r0 ⇒ {
            d.paths(e, avoid.union(r0.visited), Set(t0)).filter(r0.canCombine(_)).map(r0 ++ _)
          })
          if (augmented_routings.isEmpty) {
            (rank, quasi_routings)
          } else {
            (rank + 1, augmented_routings)
          }
        }
      }))

    new BasisMatroid[V](e, quasi_routings.map(_.sources), rank)
    /*
      About this algorithm:
      If the set t is empty, then there is only one base, which is the empty set,
      and the routine above obviously returns the right matroid.

      Assume that the algorithm works for t', we have to show that it also works
      for t'+{t0}.

      W.l.o.g. by possibly adding a dummy vertex, connecting it to t0, renaming,
      and doing a swap operation (introduced in Mason, 1972); we may assume that
      t0 is not in e and a sink in (the possibly altered) digraph d.
      If the paths expression in the foldLeft-loop is always empty, then there
      is no way of reaching any element of e that avoids any routing for a base
      of gamma(d,t',e), so  gamma(d,t',e) == gamma(d,t'+{t0},e)  and the result
      is correct.

      If the paths expression is not empty, then this proves that at least one maximal
      routing with respect to (d,t'+{t0},e) has one more path than the maximal
      routings with respect to (d,t',e), thus all bases of gamma(d,t'+{t0},e) have
      a higher cardinality than those of gamma(d,t',e); thus all maximal routings with
      respect to (d,t'+{t0},e) have a path ending in t0.


      If you are new to matroid theory: because every base of gamma(d,t',e) is
      at least an independent set of gamma(d,t'+{t0},e), and every independent
      set of a matroid may be extended to a basis of that matroid, we do not have
      to worry about routings that belong to independent sets that are non-bases
      of gamma(d,t',e).
      Certainly, we will miss some valid basis-routings this way, but not any bases.
     */
  }

}
