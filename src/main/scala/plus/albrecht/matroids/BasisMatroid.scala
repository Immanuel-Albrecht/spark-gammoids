package plus.albrecht.matroids

class BasisMatroid[T](val _ground_set : Set[T],
                      val _basis_family : Set[Set[T]],
                      val _rank : Int
                     ) extends traits.BasisMatroid[T] {

  override def groundSet(): Iterable[T] = _ground_set
  override def rank(): Int = _rank

  override def basisFamily(): Iterable[Set[T]] = _basis_family
  override def isBasis(set: Iterable[T]): Boolean = {
    val X = set.toSet
    if (X.size == _rank) {
      _basis_family contains X
    } else false
  }

  def this(basis_family : Set[Set[T]]) {
    this(basis_family.foldLeft(Set[T]())(_ ++ _),
      basis_family,
      basis_family.headOption.getOrElse(Set[T]()).size)
  }

}
