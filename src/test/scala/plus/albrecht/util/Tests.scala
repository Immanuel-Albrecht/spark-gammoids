package plus.albrecht.util

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import plus.albrecht.run.{Config, Spark}
import plus.albrecht.util.spark.Types

class Tests extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    Config(x ⇒
      x.setTagSet(Set("spark"))
        .set("master", "local[4]")
        .set("app-name", "run.Tests")
    )
  }

  "Spark" should "give a session object != null" in {
    assert(Spark.spark != null)
  }

  "Types" should "work as expected" in {

    case class badType(int: Int) {}

    assertThrows[Exception](Types.getSparkType[badType]())

    assert(null != Types.getSparkType[String]())
    assert(null != Types.getSparkType[Int]())
    assert(null != Types.getSparkType[Long]())
    assert(null != Types.getSparkType[Float]())
    assert(null != Types.getSparkType[Double]())
    assert(null != Types.getSparkType[Boolean]())

  }
}
