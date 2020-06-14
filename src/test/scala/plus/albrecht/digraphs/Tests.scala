package plus.albrecht.digraphs

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class Tests extends AnyFlatSpec with Matchers {

  "QuasiPaths.isValid" should "work" in {
    assert(QuasiPath(1, Set(1, 2, 3), 3).isValid().passed == true)
    assert(QuasiPath(1, Set(1, 2, 3), 4).isValid().passed == false)
    assert(QuasiPath(4, Set(1, 2, 3), 3).isValid(true).passed == false)
    assert(QuasiPath(4, Set(1, 2, 3), 6).isValid(false).passed == false)
  }

  lazy val d = Digraph(Set((1, 2), (2, 3), (2, 4), (4, 1)))

  "Digraph.isValid" should "work" in {
    val vs = Set(1, 2, 3)
    val wrongVs = Set(1)
    val inc = Map(1 → Set(2))
    val invInc = Map(2 → Set(1))
    val wrongIncs = Map(3 → Set(2)) :: Map[Int, Set[Int]]() :: Nil

    assert(d.isValid().passed == true)
    assert(new Digraph(vs, inc, invInc).isValid().passed == true)
    assert(new Digraph(wrongVs, inc, invInc).isValid().passed == false)
    wrongIncs.foreach(
      {
        case x ⇒ {
          assert(new Digraph(vs, x, invInc).isValid(false).passed == false)
          assert(new Digraph(vs, inc, x).isValid(false).passed == false)
        }
      }
    )
  }

  "QuasiRouting.isValid" should "work" in {
    val good = new QuasiRouting[Int](Set(1, 2, 3), Set(1, 2, 3, 4, 5, 6), Set(1, 5, 6))
    assert(good.isValid().passed)
    assert((new QuasiRouting[Int](Set(1, 2, 3), Set(1, 2, 3, 4, 5, 6), Set(5, 6))).isValid().passed == false)
    assert((new QuasiRouting[Int](Set(1, 2, 3), Set(1, 2, 3, 4, 6), Set(1, 5, 6))).isValid().passed == false)
    assert((new QuasiRouting[Int](Set(1, 2, 3), Set(1, 3, 4, 5, 6), Set(1, 5, 6))).isValid().passed == false)
  }

  val p = QuasiPath(1 :: 2 :: 3 :: 4 :: Nil)
  val p2 = QuasiPath(7 :: 6 :: Nil)
  val p3 = QuasiPath(4 :: 5 :: Nil)

  val q = QuasiRouting() ++ p
  val q2 = QuasiRouting() ++ p2
  val q3 = QuasiRouting() ++ p3
  val q4 = QuasiRouting() ++ p2 ++ p3

  "QuasiRouting.canCombine" should "work" in {

    assert(q.canCombine(p2) == true)
    assert(q.canCombine(p3) == false)
    assert(q2.canCombine(p) == true)
    assert(q2.canCombine(p3) == true)
    assert(q3.canCombine(p) == false)
    assert(q3.canCombine(p2) == true)

    assert(q.canCombine(q2) == true)
    assert(q.canCombine(q3) == false)
    assert(q2.canCombine(q) == true)
    assert(q2.canCombine(q3) == true)
    assert(q3.canCombine(q) == false)
    assert(q3.canCombine(q2) == true)

    assert(q4.canCombine(q4) == false)
    assert(q4.canCombine(q) == false)
    assert(q4.canCombine(q2) == false)
    assert(q4.canCombine(q3) == false)

  }

  "QuasiRouting.++" should "work" in {
    assert(q.sources == Set(p.source))
    assert(q.targets == Set(p.target))
    assert(q.visited == p.visited)
    assert(q2 ++ q3 == q4)
  }

  "Digraph.allPaths" should "work" in {
    val paths_d = Set(
      List(1), List(1, 2), List(1, 2, 3), List(1, 2, 4),
      List(2), List(2, 3), List(2, 4), List(2, 4, 1),
      List(3),
      List(4), List(4, 1), List(4, 1, 2), List(4, 1, 2, 3))

    assert(d.allPaths == paths_d)
    assert(d.allPathStats == paths_d.map(QuasiPath(_)).toSet)
  }

  "Digraph companion" should "create valid Digraphs" in {
    val dg0 = Digraph((1, 2) :: (2, 3) :: Nil)
    val dg1 = new Digraph(dg0.vertexSet, dg0.incidenceSets, dg0.invIncidenceSets)
    assert(dg1.isValid().passed == true)
  }

  lazy val d4 = Digraph((1 to 4).flatMap(x ⇒ (1 to 4).map((x, _))))

  "paths" should "give correct results in full digraph with 4 vertices" in {
    assert(d4.vertices() == Set(1, 2, 3, 4))

    assert(d4.paths(Set(1), Set(), Set(2)).size == 1 /* 1 arc */ +
      2 /* 2 arcs via 3 or 4 */ +
      1 /* 3 arcs via 3 and 4;
                                                 the two paths have the same QuasiPath
                                                 */
    )
    assert(d4.paths(Set(1), Set(3), Set(2)).size == 1 /* 1 arc */ +
      1 /* 2 arcs via  4 */
    )

  }

  "Digraph.opp()" should "act like identity on a full digraph" in {
    assert(d4 == d4.opp())
  }

  /* opp.opp should reflect this */
  d4.opp().opp() should be theSameInstanceAs d4

  "Digraph" should "not equal some object of a different type" in {
    assert(d4 != Set(1, 2))
  }
}
