package plus.albrecht.digraphs

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class Tests extends AnyFlatSpec with Matchers {

  "PathStats.isValid" should "work" in {
    assert(PathStats(1, Set(1, 2, 3), 3).isValid().passed == true)
    assert(PathStats(1, Set(1, 2, 3), 4).isValid().passed == false)
    assert(PathStats(4, Set(1, 2, 3), 3).isValid(true).passed == false)
    assert(PathStats(4, Set(1, 2, 3), 6).isValid(false).passed == false)
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

  "Digraph.allPaths" should "work" in {
    val paths_d = Set(
      List(1), List(1, 2), List(1, 2, 3), List(1, 2, 4),
      List(2), List(2, 3), List(2, 4), List(2, 4, 1),
      List(3),
      List(4), List(4, 1), List(4, 1, 2), List(4, 1, 2, 3))

    assert(d.allPaths == paths_d)
    assert(d.allPathStats == paths_d.map(PathStats(_)).toSet)
  }

  "Digraph companion" should "create valid Digraphs" in {
    val dg0 = Digraph((1, 2) :: (2, 3) :: Nil)
    val dg1 = new Digraph(dg0.vertexSet, dg0.incidenceSets, dg0.invIncidenceSets)
    assert(dg1.isValid().passed == true)
  }
}
