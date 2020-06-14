package plus.albrecht.matroids

import plus.albrecht.matroids.from_sage.SageNamedMatroids
import plus.albrecht.util.Lazy

import scala.io.Source

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

  /** lazy loader for matroids exported from sagemath.org's sage.matroids.named_matroids */
  lazy val from_sage : Map[String, Lazy[BasisMatroid[String]]] = {
    SageNamedMatroids.baseFamilies.map(
      {
        case (name, res) ⇒ {
          lazy val bases = Source.fromResource(f"from_sage/${res}")
            .getLines()
            .toList
            .map(_.toList.map(_.toString).toSet)

          name.toUpperCase() → Lazy(BasisMatroid(bases))
        }
      }
    ).toMap
  }

  val aliasList: Map[String, String] = Map(
    "MK4" → "M(K4)",
    "MK_4" → "M(K4)",
    "M(K_4)" → "M(K4)"
  )

  def apply(name: String): BasisMatroid[String] = {
    val upperName = name.toUpperCase()
    aliasList.getOrElse(upperName, upperName) match {
      case "M(K4)" ⇒ mk4
      case x ⇒ {
        if (from_sage contains x) {
          from_sage(x)()
        } else
        throw new Exception(f"Unknown matroid: ${name}.")
      }
    }
  }
}
