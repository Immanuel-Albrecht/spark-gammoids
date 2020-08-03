package plus.albrecht.algorithms

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class Tests extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  "findMinimalLinearOrder" should "find the correct minimal order" in {
    val perturbed = Seq(0, 1, 5, 3, 6, 2, 7, 4)
    val groundSet = perturbed.toSet
    val b0s =
      groundSet
        .subsets(3)
        .map(x ⇒ x.map(perturbed(_)).toList.permutations.map(_.toSeq))
        .toSeq
        .map(_.toIterable)

    val y = FindMinimalLinearOrder(
      groundSet,
      b0s,
      (x: Seq[Int]) ⇒ x.toIterable,
      groundSet.size - 3
    )

    assert(y == Seq(0, 1, 2, 3, 4, 5, 6, 7).reverse)
  }

}
