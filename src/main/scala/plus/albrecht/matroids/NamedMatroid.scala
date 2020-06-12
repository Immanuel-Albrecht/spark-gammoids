package plus.albrecht.matroids

/**
 * some matroids have special names, and we keep track of them here.
 */
object NamedMatroid {

  lazy val mk4 = new BasisMatroid[String](Set(
    Set("a", "b", "d"),
    Set("a", "b", "e"),
    Set("a", "b", "f"),
    Set("a", "c", "d"),
    Set("a", "c", "e"),
    Set("a", "c", "f"),
    Set("a", "d", "e"),
    Set("a", "d", "f"),
    Set("b", "c", "d"),
    Set("b", "c", "e"),
    Set("b", "c", "f"),
    Set("b", "d", "f"),
    Set("b", "e", "f"),
    Set("c", "d", "e"),
    Set("c", "e", "f"),
    Set("d", "e", "f")))

  val aliasList: Map[String, String] = Map(
    "MK4" → "M(K4)",
    "MK_4" → "M(K4)",
    "M(K_4)" → "M(K4)"
  )

  def apply(name: String): BasisMatroid[String] = {
    aliasList.getOrElse(name, name) match {
      case "M(K4)" ⇒ mk4
      case _ ⇒ throw new Exception(f"Unknown matroid: ${name}.")
    }
  }
}
