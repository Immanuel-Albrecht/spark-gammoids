package plus.albrecht.matroids

/**
 * class that stores a matroid as a family of bases (in scala)
 *
 * @param _ground_set
 *
 * @param _basis_family
 *
 * @param _rank
 *
 * @tparam T matroid element type
 */
class BasisMatroid[T](val _ground_set: Set[T],
                      val _basis_family: Set[Set[T]],
                      val _rank: Int
                     ) extends traits.BasisMatroid[T]
  with traits.RankMatroid[T] {

  override def groundSet(): Iterable[T] = _ground_set

  override def rank(): Int = _rank

  override def basisFamily(): Iterable[Set[T]] = _basis_family

  override def isBasis(set: Iterable[T]): Boolean = {
    val X = set.toSet
    if (X.size == _rank) {
      _basis_family contains X
    } else false
  }

  def this(basis_family: Set[Set[T]]) {
    this(basis_family.foldLeft(Set[T]())(_ ++ _),
      basis_family,
      basis_family.headOption.getOrElse(Set[T]()).size)
  }

  /**
   * Gives a subfamily of bases that contain a certain element.
   * Elements that are loops in the matroid will not be in the keySet.
   */
  lazy val basesContaining: Map[T, Set[Set[T]]] = {
    basisFamily().foldLeft(Map[T, Set[Set[T]]]())({
      case (m, b) ⇒
        b.foldLeft(m)({
          case (m, x) ⇒
            m ++ Map(x → (m.getOrElse(x, Set[Set[T]]()) ++ Set(b)))
        })
    })
  }

  override def equals(that: Any): Boolean = that match {
    case basisMatroid: traits.BasisMatroid[T] ⇒ {
      (
        basisMatroid.rank() == _rank
        ) && (
        basisMatroid.groundSetAsSet == _ground_set
        ) && (
        basisMatroid.basisFamily().toSet == _basis_family
        )
    }
    case _ ⇒ false
  }


  override def rk(x: Iterable[T]): Int = {
    /* the rank of x is the maximal cardinality of the intersection of x
       with any of the bases of the matroid */


    x.toSet /* We have to convert x to a set because otherwise we would
                miscalculate the rank when an element occurs more than once!
    */ .foldLeft((0, _basis_family))({
      case ((r0, fam0), e) ⇒
        val fam1 = basesContaining.getOrElse(e, Set[Set[T]]()).intersect(fam0)
        if (fam1.isEmpty) {
          /* e is in the closure of the previous elements */
          (r0, fam0)
        } else {
          /* e is not in the closure */
          (r0 + 1, fam1)
        }
    })._1
    /* So, how exactly does this work, one might ask?
       The above fold operation implicitly determines a maximal independent
       subset of x; which is the set consisting of those elements that
       reach 'e is not in the closure'. In the tuple (r0,fam0), r0 keeps track
       of the cardinality of the implicit maximal independent set with respect
       to all previous elements, and fam0 keeps track of all bases that contain
       this implicit set. So fam1 clearly consists of those bases of fam0
       that also contain e.

       Remember: all inclusion maximal independent subsets of X in a matroid
       have the same cardinality and thus the greedy approach works.
     */
  }

}

/**
 * companion object
 */
object BasisMatroid {

  /**
   * convenience constructor
   *
   * @param bases a non-empty family of bases
   *
   * @tparam T
   *
   * @return BasisMatroid
   */
  def apply[T](bases: Iterable[Set[T]]): BasisMatroid[T] = {
    val bs: Set[Set[T]] = bases.toSet
    val groundSet: Set[T] = bs.foldLeft(Set[T]())(_ ++ _)

    val rank: Int = bs.head.size

    new BasisMatroid[T](groundSet, bs, rank)
  }

}