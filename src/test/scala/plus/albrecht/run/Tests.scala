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

}
