package plus.albrecht.iterables

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import plus.albrecht.run.Config

class Tests extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    Config(
      x ⇒
        x.setTagSet(Set("spark"))
          .set("master", "local[4]")
          .set("app-name", "digraphs.Tests")
    )
  }

  "OrderedMChooseN" should "produce the correct amount of subsets in the correct order" in {
    ((4, 1, 0) ::
      (4, 1, 1) ::
      (4, 2, 0) ::
      (6, 4, 0) ::
      (4, 2, 3) ::
      (6, 4, 0) ::
      (6, 4, 3) ::
      (6, 4, 4) ::
      (6, 4, 5) :: Nil).foreach({
      case (m, n, m0) ⇒ {
        val mcn =
          if (m0 == 0) OrderedMChooseN(m, n) else OrderedMChooseN(m, n, m0)
        val m_choose_n = (BigInt(m - n + 1) to m).product / (BigInt(1) to n).product
        val m0_choose_n =
          if (m0 == 0) BigInt(0)
          else (BigInt(m0 - n + 1) to m0).product / (BigInt(1) to n).product

        println(
          s"m = ${m}, n = ${n}, m0 = ${m0}, m_choose_n = ${m_choose_n}, m0_choose_n = ${m0_choose_n}"
        )

        assert(BigInt(mcn.size) == m_choose_n - m0_choose_n)
        mcn.tail.foldLeft(mcn.head)({
          case (u, v) => {
            assert(OrderedMChooseN.cmp(u, v) == -1)
            assert(OrderedMChooseN.cmp(v, u) == 1)
            assert(OrderedMChooseN.cmp(v, v) == 0)
            v
          }
        })
      }
    })
  }

  "OrderedMChooseN" should "throw on wrong input data" in {
    assertThrows[IllegalArgumentException](OrderedMChooseN(4, 5))
    assertThrows[IllegalArgumentException](OrderedMChooseN(6, 4, 1))
    assertThrows[IllegalArgumentException](OrderedMChooseN(6, 4, 6))
  }

}
