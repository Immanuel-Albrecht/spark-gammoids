package plus.albrecht.run

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class Tests extends AnyFlatSpec with Matchers {

  "Spark" should "give a session object" in {
    assert(null != Spark.spark)
  }

}
