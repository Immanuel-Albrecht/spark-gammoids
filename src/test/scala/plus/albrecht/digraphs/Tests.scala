package plus.albrecht.digraphs

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class Tests extends AnyFlatSpec with Matchers {

  "PathStats.isValid" should "work" in {
    assert(PathStats(1,Set(1,2,3),3).isValid == true)
    assert(PathStats(1,Set(1,2,3),4).isValid == false)
    assert(PathStats(4,Set(1,2,3),3).isValid == false)
    assert(PathStats(4,Set(1,2,3),6).isValid == false)
  }
}
