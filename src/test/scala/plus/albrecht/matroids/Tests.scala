package plus.albrecht.matroids


import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class Tests extends AnyFlatSpec with Matchers {

  "NamedMatroids" should "be valid" in {
    val names: Set[String] = NamedMatroid.aliasList.map({ case (_, name) ⇒ name }).toSet
    names.foreach(
      (name: String) ⇒ {
        assert(NamedMatroid(name).isValid().passed == true)
      }
    )
  }

  "MK4.rk" should "give correct rank" in {
    val mk4 = NamedMatroid("MK4")

    mk4._ground_set.subsets().foreach(x ⇒ {
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
    }
    )
  }

  "MK4" should "be self-dual" in {
    /* note that M(K4) is not identically self-dual */
    val phi = Map("a" → "d", "b" → "f", "c" → "e",
      "d" → "a", "f" → "b", "e" → "c")
    val mk4 = NamedMatroid("MK4")

    assert(mk4.rank() == mk4.`rank*`())

    mk4.groundSetAsSet.subsets().foreach(x ⇒ {
      val x_phi = x.map(phi(_))
      assert(mk4.rk(x) == mk4.`rk*`(x_phi))
      assert(mk4.isBasis(x) == mk4.`isBasis*`(x_phi))
    }
    )
  }

  "BasisMatroid.equals" should "work" in {
    val mk4 = NamedMatroid("MK4")
    val m = new BasisMatroid[String](mk4.groundSet().toSet, mk4.basisFamily().toSet, mk4.rank())
    val m2 = new BasisMatroid[String](mk4.groundSet().toSet, mk4.basisFamily().toSet ++ Set(Set("a", "b", "c")).toSet, mk4.rank())
    /* m2 is the hyperplane-circuit relaxation of {a,b,c} in M(K4) */

    assert(m == mk4)
    assert(m2 != mk4)
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
      Set("d", "e", "f") → true)

    checkBasis.foreach({
      case (x, isBase) ⇒ assert(NamedMatroid.mk4.isBasis(x) == isBase)
    })
  }

  "B2-test" should "detect problems" in {
    val not_mk4 = new BasisMatroid[String](Set(
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
      Set("d", "e", "f")))

    assert(not_mk4.isValid().passed == false)
  }

  "Unknown matroid name" should "throw" in {
    try {
      NamedMatroid("oeu]+oeugoensthoeu!jq]]oeu]]")
      fail("Garbled matroid name did not throw!")
    }
    catch {
      case _: Exception ⇒ Unit
    }
  }

}
