package plus.albrecht.matroids

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{
  DataType,
  IntegerType,
  StructType,
  StringType
}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import plus.albrecht.digraphs.Digraph
import plus.albrecht.matroids.spark.SparkBasisMatroid
import plus.albrecht.matroids.spark.adapters.BasisToSparkMatroid
import plus.albrecht.matroids.spark.traits.SparkMatroid
import plus.albrecht.matroids.tests.axioms.traits.AxiomTest
import plus.albrecht.run.{Config, Spark}

class Tests extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    Config(x ⇒
      x.setTagSet(Set("spark"))
        .set("master", "local[4]")
        .set("app-name", "matroids.Tests")
    )
  }

  "Spark" should "give a session object != null" in {
    assert(Spark.spark != null)
  }

  "Gammoid" should "return the correct matroid" in {
    val arcs = List(
      ("a", "e"),
      ("b", "f"),
      ("c", "g"),
      ("d", "h"),
      ("d", "i"),
      ("e", "f"),
      ("e", "g"),
      ("e", "h"),
      ("e", "i"),
      ("f", "e"),
      ("f", "g"),
      ("f", "h"),
      ("f", "j"),
      ("g", "e"),
      ("g", "f"),
      ("g", "h"),
      ("g", "k"),
      ("h", "e"),
      ("h", "f"),
      ("h", "g"),
      ("h", "l"),
      ("j", "m"),
      ("k", "m"),
      ("m", "k")
    )
    val targets = List("i", "j", "k", "l", "m")
    val edges = Set("a", "b", "c", "d", "e", "f", "g", "h", "k")
    val bases = Set(
      Set("a", "c", "b", "d"),
      Set("c", "b", "e", "d"),
      Set("a", "c", "d", "f"),
      Set("c", "e", "d", "f"),
      Set("a", "b", "d", "g"),
      Set("b", "e", "d", "g"),
      Set("a", "d", "g", "f"),
      Set("e", "d", "g", "f"),
      Set("a", "h", "c", "b"),
      Set("a", "h", "b", "d"),
      Set("a", "h", "c", "d"),
      Set("h", "c", "b", "d"),
      Set("h", "c", "b", "e"),
      Set("h", "b", "e", "d"),
      Set("h", "c", "e", "d"),
      Set("a", "h", "c", "f"),
      Set("a", "h", "d", "f"),
      Set("h", "c", "d", "f"),
      Set("h", "c", "e", "f"),
      Set("h", "e", "d", "f"),
      Set("a", "h", "b", "g"),
      Set("a", "h", "d", "g"),
      Set("h", "b", "d", "g"),
      Set("h", "b", "e", "g"),
      Set("h", "e", "d", "g"),
      Set("a", "h", "g", "f"),
      Set("h", "d", "g", "f"),
      Set("h", "e", "g", "f"),
      Set("a", "c", "b", "k"),
      Set("a", "k", "b", "d"),
      Set("a", "k", "c", "d"),
      Set("k", "b", "c", "d"),
      Set("c", "b", "e", "k"),
      Set("k", "b", "e", "d"),
      Set("k", "e", "c", "d"),
      Set("a", "c", "k", "f"),
      Set("a", "k", "d", "f"),
      Set("k", "f", "c", "d"),
      Set("c", "e", "k", "f"),
      Set("k", "e", "d", "f"),
      Set("a", "k", "b", "g"),
      Set("a", "k", "d", "g"),
      Set("k", "b", "d", "g"),
      Set("k", "b", "e", "g"),
      Set("k", "e", "d", "g"),
      Set("a", "k", "g", "f"),
      Set("k", "d", "g", "f"),
      Set("k", "e", "g", "f"),
      Set("a", "h", "k", "b"),
      Set("a", "h", "c", "k"),
      Set("h", "c", "b", "k"),
      Set("a", "h", "k", "d"),
      Set("h", "k", "b", "d"),
      Set("h", "c", "d", "k"),
      Set("h", "k", "b", "e"),
      Set("h", "c", "e", "k"),
      Set("h", "k", "e", "d"),
      Set("a", "h", "k", "f"),
      Set("h", "c", "k", "f"),
      Set("h", "k", "d", "f"),
      Set("h", "k", "e", "f"),
      Set("a", "h", "k", "g"),
      Set("h", "k", "b", "g"),
      Set("h", "k", "d", "g"),
      Set("h", "k", "e", "g"),
      Set("h", "k", "g", "f")
    )
    val d = Digraph(arcs)

    /* verify that our algorithm is independent on the ordering of the targets */
    targets.permutations.foreach(t ⇒ {
      val g = Gammoid(d, t, edges)
      assert(g.basisFamily().toSet == bases)
      assert(g.isValid().passed) /* counter check the isValid routine */
    })
  }

  "NamedMatroids" should "be valid" in {
    val names: Set[String] = NamedMatroid.aliasList
      .map({ case (_, name) ⇒ name })
      .toSet ++ NamedMatroid.from_sage.keySet
    names.foreach({
      case name: String ⇒ {

        if (NamedMatroid.isBig(name)) {
          println(s"${name} is big. Validity test skipped.")
        } else {

          print(s"Is ${name} valid?")
          assert(NamedMatroid(name).isValid().passed == true)
          println(" yes.")
        }
      }
    })
  }

  "SparkBasisMatroid(BasisMatroid)" should "create equivalent matroid" in {
    val mk4 = NamedMatroid.mk4
    val mk4s = BasisToSparkMatroid(mk4)

    val mk4sb = SparkBasisMatroid(mk4s)
    assert(mk4sb.groundSetAsSet == mk4.groundSetAsSet)
    assert(mk4sb.basisFamily().toSet == mk4.basisFamily().toSet)

    mk4.groundSetAsSet.subsets.foreach(x ⇒ assert(mk4.rk(x) == mk4sb.rk(x)))

  }

  "MK4.rk" should "give correct rank" in {
    val mk4 = NamedMatroid("MK4")

    mk4._ground_set
      .subsets()
      .foreach(x ⇒ {
        val r = x.size match {
          case q if q < 3 ⇒ q
          case q if q > 3 ⇒ 3
          case _ ⇒
            x match {
              case x if x == Set("a", "b", "c") ⇒ 2
              case x if x == Set("a", "e", "f") ⇒ 2
              case x if x == Set("b", "d", "e") ⇒ 2
              case x if x == Set("c", "d", "f") ⇒ 2
              case _ ⇒ 3
            }
        }
        assert(mk4.rk(x) == r)
      })
  }

  "MK4" should "be self-dual" in {
    /* note that M(K4) is not identically self-dual */
    val phi =
      Map("a" → "d", "b" → "f", "c" → "e", "d" → "a", "f" → "b", "e" → "c")
    val mk4 = NamedMatroid("MK4")

    assert(mk4.rank() == mk4.`rank*`())

    mk4.groundSetAsSet
      .subsets()
      .foreach(x ⇒ {
        val x_phi = x.map(phi(_))
        assert(mk4.rk(x) == mk4.`rk*`(x_phi))
        assert(mk4.isBasis(x) == mk4.`isBasis*`(x_phi))
      })
  }

  "BasisMatroid.equals" should "work" in {
    val mk4 = NamedMatroid("MK4")
    val m = new BasisMatroid[String](
      mk4.groundSet().toSet,
      mk4.basisFamily().toSet,
      mk4.rank()
    )
    val m2 = new BasisMatroid[String](
      mk4.groundSet().toSet,
      mk4.basisFamily().toSet ++ Set(Set("a", "b", "c")).toSet,
      mk4.rank()
    )
    /* m2 is the hyperplane-circuit relaxation of {a,b,c} in M(K4) */

    assert(m == mk4)
    assert(m2 != mk4)
    assert(m != 4)
  }

  "BasisMatroid.isValid()" should "work" in {
    val m0 = new BasisMatroid[Int](Set(1), Set(Set()), -1)

    assert(m0.isValid().passed == false)

    val m1 = new BasisMatroid[Int](Set(1), Set(Set()), 1)

    assert(m1.isValid().passed == false)

    val m2 = new BasisMatroid[Int](Set(1), Set(), 1)

    assert(m2.isValid().passed == false)

    val m3 = new BasisMatroid[Int](Set(1), Set(Set()), 0)

    assert(m3.isValid().passed == true)

    val m4 = new BasisMatroid[Int](Set(1), Set(Set(2)), 1)

    assert(m4.isValid().passed == false)

  }

  "SparkBasisMatroid.isValid()" should "work" in {

    val m1 = new BasisMatroid[Int](Set(1), Set(Set()), 1)

    assert(SparkBasisMatroid(m1).isValid().passed == false)

    val m2 = new BasisMatroid[Int](Set(1), Set(), 1)

    assert(SparkBasisMatroid(m2).isValid().passed == false)

    val m3 = new BasisMatroid[Int](Set(1), Set(Set(1)), 1)
    assert(SparkBasisMatroid(m3).isValid().passed == true)

    val m4 = new BasisMatroid[Int](Set(1), Set(Set(2)), 1)

    assert(SparkBasisMatroid(m4).isValid().passed == false)

  }

  val mk4_spark = SparkBasisMatroid(NamedMatroid.mk4)

  "MK4" should "be the right matroid" in {
    val checkBasis: Map[Set[String], Boolean] = Map(
      Set("a", "b", "c") → false,
      Set("a", "b", "d") → true,
      Set("a", "b", "e") → true,
      Set("a", "b", "f") → true,
      Set("a", "c", "d") → true,
      Set("a", "c", "e") → true,
      Set("a", "c", "f") → true,
      Set("a", "d", "e") → true,
      Set("a", "d", "f") → true,
      Set("a", "e", "f") → false,
      Set("b", "c", "d") → true,
      Set("b", "c", "e") → true,
      Set("b", "c", "f") → true,
      Set("b", "d", "e") → false,
      Set("b", "d", "f") → true,
      Set("b", "e", "f") → true,
      Set("c", "d", "e") → true,
      Set("c", "d", "f") → false,
      Set("c", "e", "f") → true,
      Set("d", "e", "f") → true
    )

    checkBasis.foreach({
      case (x, isBase) ⇒ {
        assert(NamedMatroid.mk4.isBasis(x) == isBase)
        assert(mk4_spark.isBasis(x) == isBase)
      }
    })
  }

  "B2-test" should "detect problems" in {
    val not_mk4 = new BasisMatroid[String](
      Set(
        Set("a", "b", "d"),
        Set("a", "b", "e"),
        Set("a", "b", "f"),
        Set("a", "c", "d"),
        Set("a", "c", "e"),
        Set("a", "c", "f"),
        Set("a", "d", "e"),
        Set("a", "d", "f"),
        Set("b", "c", "d"),
        // Set("b","c","e"),
        Set("b", "c", "f"),
        Set("b", "d", "f"),
        Set("b", "e", "f"),
        Set("c", "d", "e"),
        Set("c", "e", "f"),
        Set("d", "e", "f")
      )
    )

    assert(not_mk4.isValid().passed == false)
    assert(SparkBasisMatroid(not_mk4).isValid().passed == false)
  }

  "Unknown matroid name" should "throw" in {
    try {
      NamedMatroid("oeu]+oeugoensthoeu!jq]]oeu]]")
      fail("Garbled matroid name did not throw!")
    } catch {
      case _: Exception ⇒ Unit
    }
  }

  "cocanonicalBasisIndicator" should "not crash" in {
    assert(
      NamedMatroid("MK4").cocanonicalBasisIndicator.size == NamedMatroid(
        "MK4"
      ).canonicalBasisIndicator.size
    )
  }

  "isSelfDual" should "work on well-known examples" in {
    assert(NamedMatroid("MK4").isSelfDual() == true)
    assert(NamedMatroid("P8pp").isSelfDual() == true)
    assert(NamedMatroid("R6").isSelfDual() == true)
    assert(NamedMatroid("F8").isSelfDual() == true)
    assert(NamedMatroid("Fano").isSelfDual() == false)
    assert(NamedMatroid("O7").isSelfDual() == false)
    assert(NamedMatroid("P8").isSelfDual() == true)

    val M = Gammoid(
      Digraph(
        ("d", "a") ::
          ("e", "a") ::
          ("f", "a") ::
          ("b", "b") ::
          ("c", "c") :: Nil
      ),
      Set("a", "b", "c"),
      Set("a", "b", "c", "d", "e", "f")
    )

    assert(M.isSelfDual() == false)
  }

  "AxiomTest.derived.class" should "fail test if unimplemented" in {
    class subtest extends AxiomTest {
      override val failFast: Boolean = false
    }
    assert((new subtest).result.passed == false)
  }

  "SparkMatroid.default.implementations" should "give correct results" in {
    val mk4 = NamedMatroid("MK4")
    val s_mk4 = BasisToSparkMatroid(mk4)

    assert(
      SparkMatroid
        .inferGroundSetFromBasisFamily(s_mk4.dfBasisFamily)
        .collect()
        .map(r ⇒ r.getString(0))
        .toSet == mk4
        .groundSet()
    )

    assert(
      SparkMatroid
        .inferRankFromBasisFamily(s_mk4.dfBasisFamily, s_mk4.spark())
        .head
        .getInt(0) == mk4.rank()
    )

    val s = new SparkMatroid[Int] {
      override def elementType(): DataType = IntegerType

    }

    assert(s.df(SparkMatroid.dataRank) == None)
    assert(s.df(SparkMatroid.dataGroundSet) == None)
    assert(s.df(SparkMatroid.dataBasisFamily) == None)

    val s2 = new SparkMatroid[String] {
      override def elementType(): DataType = StringType

      override def df(data: String): Option[DataFrame] = {
        data match {
          case x if x == SparkMatroid.dataBasisFamily ⇒
            Some(s_mk4.dfBasisFamily)
          case _ ⇒ super.df(data)
        }
      }
    }

    assert(
      s2.df(SparkMatroid.dataGroundSet)
        .get
        .collect()
        .map(r ⇒ r.getString(0))
        .toSet == mk4
        .groundSet()
    )

    assert(
      s2.df(SparkMatroid.dataRank)
        .get
        .head
        .getInt(0) == mk4.rank()
    )

  }

  "basisIndicatorConstructor" should "work as expected" in {
    val m0 = NamedMatroid("MK4")
    val as_string = m0.toString()
    val from_string = BasisMatroid(as_string)

    assert(from_string.toString() == m0.toString())

    val can_order = m0.canonicalOrdering.toArray

    val bases: Set[Set[String]] =
      from_string.basisFamily.map(s ⇒ s.map(i ⇒ can_order(i))).toSet

    val bases2 = m0.basisFamily.toSet

    assert(bases == bases2)

  }

}
