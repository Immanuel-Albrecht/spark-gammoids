package plus.albrecht.iterables

import scala.reflect.ClassTag

/**
  * iterable object that iterates over all lists of (n) elements with entries
  * using elements from 0 to (m-1), skipping those lists that only have entries
  * using elements from 0 to (m0-1); furthermore,
  * the elements of these lists are in increasing order, and
  * the lists are traversed in reverse-lexicographic order '<.<':
  * [0, 1, 2] :: [0, 1, 3] :: [0, 2, 3] :: [1, 2, 3] :: [0, 1, 4] :: ...
  *
  * A <.< B ::if:: the biggest element in the symmetric difference
  *                of A and B belongs to B
  *
  * @param m       index set size
  *
  * @param n       list length
  *
  * @param m0      biggest element that defines the start of the sequence,
  *                must be at least (n-1)
  *
  * @tparam INT
  */
class OrderedMChooseN[INT: ClassTag](val m: INT, val n: Int, val m0: INT)(
  implicit integerType: Integral[INT]
) extends Iterable[List[INT]] {

  require(
    integerType.lteq(integerType.fromInt(n), m),
    s"m = ${m} < ${n} = n    -> Cannot choose ${n} elements from ${m}-elementary ground set!"
  )

  require(
    integerType.gteq(m0, integerType.fromInt(n - 1)),
    s"m0 = ${m0} < ${n - 1} = n - 1   -> The parameter m0 does not define a valid starting point of the sequence!"
  )

  require(
    integerType.lt(m0, m),
    s"m0 = ${m0} >= ${m} = m   -> The start of the sequence is not within its scope!"
  )

  def this(m: INT, n: Int)(implicit integerType: Integral[INT]) {
    this(m, n, integerType.plus(integerType.zero, integerType.fromInt(n - 1)))
  }

  class IterObject(var x0: Option[Array[INT]], val m: INT, val n: Int)
      extends Iterator[List[INT]] {

    def find_update_index(i0: Int): Int = {
      if (i0 == n - 1)
        i0
      else {
        if (integerType.lt(
              integerType.plus(x0.get(i0), integerType.fromInt(1)),
              x0.get(i0 + 1)
            )) {
          i0
        } else {
          find_update_index(i0 + 1)
        }
      }
    }

    /**
      * obtain the next element:
      *   find the first element x_i with the property that
      *     if the element has a successor, then x_i + 1 < x_{i+1}.
      */
    def find_update_index(): Int = find_update_index(0)

    override def hasNext: Boolean = x0.isDefined

    override def next(): List[INT] = {
      val element = x0.get.toList

      val idx = find_update_index()

      x0.get(idx) = integerType.plus(x0.get(idx), integerType.fromInt(1))
      (0 to (idx - 1)).foreach(i ⇒ x0.get(i) = integerType.fromInt(i))

      /* check whether we reached the end of the requested sequence */
      if (integerType.gteq(x0.get(n - 1), m))
        x0 = None

      element
    }
  }

  override def iterator: Iterator[List[INT]] = {
    val first =
      (Range(0, n - 1)
        .map(i ⇒ integerType.fromInt(i))
        .toSeq ++ Seq[INT](m0)).toArray

    new IterObject(Some(first), m, n)
  }

}

/** companion object */
object OrderedMChooseN {

  /**
    * Creates a collection of ordered subsets of cardinality n wrt.
    * to a ground set of cardinality m.
    *
    * @param m            number of elements in the ground set
    * @param n            number of elements chosen from the ground set
    * @param m0           lower bound for the biggest element chosen (>= n-1)
    * @param integerType
    * @tparam INT         integer type of the ground set
    * @return  OrderedMChooseN collection
    */
  def apply[INT: ClassTag](m: INT, n: Int, m0: INT)(
    implicit integerType: Integral[INT]
  ) =
    new OrderedMChooseN(m, n, m0)

  /**
    * Creates the collection of all ordered subsets of cardinality n wrt.
    * to a ground set of cardinality m.
    *
    * @param m            number of elements in the ground set
    * @param n            number of elements chosen from the ground set
    * @param integerType
    * @tparam INT         integer type of the ground set
    * @return  OrderedMChooseN collection
    */
  def apply[INT: ClassTag](m: INT,
                           n: Int)(implicit integerType: Integral[INT]) =
    new OrderedMChooseN(m, n)

  /**
    * compares to ordered lists with respect to the reverse-lexicographic order
    *
    * @param a           ordered list
    * @param b           ordered list
    * @param integerType
    * @tparam INT        integer type of list elements
    * @return   0, if a == b wrt. reverse-lexicographic order on sets,
    *          -1, if a < b,
    *           1, if a > b.
    */
  def cmp[INT: ClassTag](a: List[INT], b: List[INT])(
    implicit integerType: Integral[INT]
  ): Int = {
    val as = a.toSet
    val bs = b.toSet
    val diff = as.diff(bs) ++ bs.diff(as)

    if (diff.isEmpty) 0
    else {
      val max_element = diff.tail.foldLeft(diff.head)({
        case (x, y) ⇒ integerType.max(x, y)
      })

      if (bs contains max_element)
        -1
      else
        1
    }
  }
}
