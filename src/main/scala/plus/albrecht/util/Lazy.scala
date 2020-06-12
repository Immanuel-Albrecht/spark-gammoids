package plus.albrecht.util

/**
 * Lazy evaluation wrapper with built-in caching.
 *
 * @param value  value expression (evaluated later)
 * @tparam T
 */
class Lazy[T](value : ⇒ T) {

  lazy val _value = value

  /**
   * @return the desired value
   */
  def apply() : T = _value

}

/**
 * lazy companion :)
 */
object Lazy {

  /**
   * create a lazy wrapper
   * @param value  expression
   * @tparam T
   */
  def apply[T](value : ⇒ T) : Lazy[T] = {
    new Lazy[T](value)
  }

}