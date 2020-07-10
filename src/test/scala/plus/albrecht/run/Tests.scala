package plus.albrecht.run

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class Tests extends AnyFlatSpec with Matchers {

  "Spark" should "give a session object != null" in {
    assert(Spark.spark != null)
  }

  "TestMain" should "not fail with --unit-test" in {
    TestMain.main(Array("--unit-test"))
  }

  "IsNamedMatroidVaild" should "not fail on P8pp" in {
    IsNamedMatroidValid.main(Array("P8pp"))
  }

  "Config" should "work as expected" in {
    val cfg0 = new Config() << ("a", "a") << ("b", "b") << ("c", "c")
    val cfg1 = new Config() << ("a", "A") << ("b", "B") << ("c", "C")
    Config(_ ++ "lower" << cfg0)
    Config(_ ++ "upper" << cfg1)
    Config(_ << ("b", "_"))
    Config(_ ++ Set("aux") << ("c", "_"))
    Config(_ ++ Set("aux") << ("d", "XX"))

    assert(Config() >> "a" === None)
    assert(Config() >> "other" === None)
    assert(Config() ++ "aux" >> "d" === Some("XX"))
    assert(Config() >> "d" === None)
    assert(Config() ++ "aux" >> "a" === None)
    assert(Config() ++ "aux" >> ("a", 12) === 12)
    assert(Config() >> "b" === Some("_"))
    assert(Config() ++ "aux" >> "b" === Some("_"))
    assert(Config() ++ "lower" >> "a" === Some("a"))
    assert(Config() ++ "upper" >> "a" === Some("A"))
    assert(Config() ++ "lower" ++ "aux" >> "a" === Some("a"))
    assert(Config() ++ "upper" ++ "aux" >> "a" === Some("A"))
    assert(Config() ++ "lower" >> "b" === Some("b"))
    assert(Config() ++ "upper" >> "b" === Some("B"))
    assert(Config() ++ "lower" ++ "aux" >> "b" === Some("b"))
    assert(Config() ++ "upper" ++ "aux" >> "b" === Some("B"))
    assert((Config() ++ "lower" ++ "aux" ^ ()) >> "b" === Some("_"))
    assert((Config() ++ "lower" ++ "aux" ^ "upper") >> "b" === Some("B"))
    assert((Config() ++ "lower" ++ "aux" -- "lower") >> "b" === Some("_"))
    assert((Config() ++ "lower" ++ "aux" ^ Set("upper")) >> "b" === Some("B"))
    assert((Config() ++ Set("lower", "aux") -- "lower") >> "c" === Some("_"))
    assert(
      (Config() ++ Set("upper", "aux") -- "lower" -- "aux") >> "c" === Some("C")
    )
    assert(
      (Config() ++ Set("upper", "aux") -- Set("upper", "aux") >> "c" === None)
    )

  }
}
