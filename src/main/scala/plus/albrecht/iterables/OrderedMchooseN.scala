package plus.albrecht.iterables

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
  * @param n       list length
  * @param m0
  * @tparam INT
  */
class OrderedMChooseN[INT](val m: INT, val n: Int, val m0: INT)(
  implicit integerType: Integral[INT]
) extends Iterable[List[INT]] {

  def this(m: INT, n: INT)(implicit integerType: Integral[INT]) {
    this(m, n, integerType.plus(integerType.zero, integerType.fromInt(n - 1)))
  }

  class IterObject(var x0: Array[INT], val m: INT, val n: Int)
      extends Iterator[List[INT]] {
    override def hasNext: Boolean = {
      x0.forall(k ⇒ integerType.gteq(k, integerType.minus(m, n)))
    }

    override def next(): List[INT] = {
      throw new Exception("TODO: IMPLEMENT!")
      x0.toList
    }
  }

  override def iterator: Iterator[List[INT]] = {
    val first =
      (Range(0, n - 2)
        .map(i ⇒ integerType.plus(m, integerType.fromInt(i)))
        .toSeq ++ Seq[INT](m0)).toArray

    new IterObject(first, m, n)
  }

}
