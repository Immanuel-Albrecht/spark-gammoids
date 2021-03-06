package plus.albrecht.matroids.from_sage

/** This object contains the named matroids from the SageMATH project (sagemath.org). */
object SageNamedMatroids {

  /**
   * We consider these matroids to be rather big.
   */
  val bigMatroids = Set[String]("ExtendedBinaryGolayCode", "Terrahawk", "D16")


  /**
   * map matroid names to file names where their base families are stored.
   */
  val baseFamilies = Map[String, String](
    "AG23-" -> "AG23minus.bases.txt",
    "AG32'" -> "AG32prime.bases.txt",
    "BetsyRoss" -> "BetsyRoss.bases.txt",
    "Block_10_5" -> "Block_10_5.bases.txt",
    "Block_9_4" -> "Block_9_4.bases.txt",
    "D16" -> "D16.bases.txt",
    "ExtendedBinaryGolayCode" -> "ExtendedBinaryGolayCode.bases.txt",
    "ExtendedTernaryGolayCode" -> "ExtendedTernaryGolayCode.bases.txt",
    "F8" -> "F8.bases.txt",
    "Fano" -> "Fano.bases.txt",
    "J" -> "J.bases.txt",
    "K33*" -> "K33dual.bases.txt",
    "L8" -> "L8.bases.txt",
    "N1" -> "N1.bases.txt",
    "N2" -> "N2.bases.txt",
    "NonFano" -> "NonFano.bases.txt",
    "NonPappus" -> "NonPappus.bases.txt",
    "NonVamos" -> "NonVamos.bases.txt",
    "NotP8" -> "NotP8.bases.txt",
    "O7" -> "O7.bases.txt",
    "P6" -> "P6.bases.txt",
    "P7" -> "P7.bases.txt",
    "P8" -> "P8.bases.txt",
    "P8pp" -> "P8pp.bases.txt",
    "P9" -> "P9.bases.txt",
    "Pappus" -> "Pappus.bases.txt",
    "Q10" -> "Q10.bases.txt",
    "Q6" -> "Q6.bases.txt",
    "Q8" -> "Q8.bases.txt",
    "R10" -> "R10.bases.txt",
    "R12" -> "R12.bases.txt",
    "R6" -> "R6.bases.txt",
    "R8" -> "R8.bases.txt",
    "R9A" -> "R9A.bases.txt",
    "R9B" -> "R9B.bases.txt",
    "S8" -> "S8.bases.txt",
    "T12" -> "T12.bases.txt",
    "T8" -> "T8.bases.txt",
    "TernaryDowling3" -> "TernaryDowling3.bases.txt",
    "Terrahawk" -> "Terrahawk.bases.txt",
    "TicTacToe" -> "TicTacToe.bases.txt",
    "Vamos" -> "Vamos.bases.txt")
}
