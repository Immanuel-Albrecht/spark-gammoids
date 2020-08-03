package plus.albrecht.algorithms

import scala.reflect.ClassTag

/**
  * This object contains an abstract algorithm for finding a
  * linear order on a set of elements with respect to an element of
  * an induced order.
  *
  * It will be used to determine a linear order of matroid elements
  * that minimizes the basis-indicator vector with respect to the induced
  * order on the rank-elementary subsets of the groundset.
  */
object FindMinimalLinearOrder {

  def apply[T, P](
      groundSet: Set[T],
      b0s: Seq[Iterable[Seq[T]]],
      objective_vector: (Seq[T] ⇒ P),
      extend_by: Int
  )(implicit
      p: Ordering[P]
  ): Seq[T] = {

    case class orderExtension(
        nextB0: Iterable[Seq[T]],
        b0: Seq[T],
        s1: Seq[T]
    ) {}

    /* we iterate using a tail recursion on this function */
    def iterate(
        to_do: Seq[orderExtension],
        maximal: Seq[orderExtension],
        maximum: Option[P],
        nbr_items_missing: Int
    ): Seq[T] = {
      if (to_do.isEmpty) {
        if (nbr_items_missing == 0) {
          /* we are done. just return the first maximal linear order. */
          maximal.head.b0 ++ maximal.head.s1
        } else {
          /* We extend the lists by one element each. */

          iterate(
            maximal.flatMap({
              case orderExtension(nextB0, b0, s1) ⇒
                groundSet
                  .diff((b0 ++ s1).toSet)
                  .map((t: T) => orderExtension(nextB0, b0, s1 :+ t))
                  .toSeq
            }),
            Seq[orderExtension](),
            None,
            nbr_items_missing - 1
          )
        }
      } else {
        /* We have to filter the elements of the to_do sequence.
         */
        (
            (x: (Seq[orderExtension], Seq[orderExtension], Option[P])) ⇒ {

              iterate(x._1, x._2, x._3, nbr_items_missing)
              /* There has to be a nicer way to fold on the parameters that are
                 passed to the next iteration step than this here.
               */
            }
        )(
          to_do
            .foldLeft(
              (Seq[orderExtension](), maximal, maximum)
            )(
              {
                case (
                      (to_do, maximal, maximum),
                      x @ orderExtension(nextB0, b0, s1)
                    ) ⇒ {

                  if (maximum.isDefined) {

                    val m = maximum.get
                    val c = objective_vector(b0 ++ s1)
                    p.compare(m, c) match {
                      case -1 ⇒ {
                        /* m < c */
                        /* we found a new maximum, all old maximal elements are
                          no longer maximal. */
                        (
                          to_do ++ maximal,
                          Seq(x),
                          Some(c)
                        )
                      }
                      case 0 ⇒ {
                        /* m == c */
                        /* append to the set of maximal solutions */
                        (to_do, maximal :+ x, maximum)
                      }
                      case 1 ⇒ {
                        /* m > c */
                        /* b0 ++ s1 is not maximal; we might try a differently
                         * ordered alternative */
                        if (nextB0.isEmpty) {
                          /* we ruled out all permutations of b0, thus we
                          now remove x from further consideration.
                           */
                          (to_do, maximal, maximum)
                        } else {
                          /* add the next permutation of b0
                             to the to_do sequence
                           */
                          (
                            to_do :+ orderExtension(
                              nextB0.tail,
                              nextB0.head,
                              s1
                            ),
                            maximal,
                            maximum
                          )
                        }
                      }
                    }
                  } else {
                    /* no maximum has not been assigned, check that maximal is
                     * indeed empty */
                    assert(maximal.isEmpty)

                    /* so the first candidate encountered is the maximal one
                    so far...
                     */
                    (
                      to_do,
                      Seq[orderExtension](x),
                      Some(objective_vector(b0 ++ s1))
                    )
                  }
                }
              }
            )
        )

      }
    }

    iterate(
      b0s.map(next ⇒
        orderExtension(
          next.tail,
          next.head,
          Seq[T]()
        )
      ),
      Seq[orderExtension](),
      None,
      extend_by
    )
  }

}
