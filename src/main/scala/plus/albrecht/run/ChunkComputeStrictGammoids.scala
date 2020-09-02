package plus.albrecht.run

object ChunkComputeStrictGammoids {

  /**
    * biggest dipath id in our database of iso-representatives of all weakly
    * connected digraphs with <= 10 arcs.
    */
  val max_dipath_id: Int = 2677189

  /**
    * default chunk size
    */
  val default_chunk_size: Int = 500

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println(
        "ERROR: we expect exactly two arguments: first, the location" +
          "of the path set family parquet, and second, the target location for the" +
          "arc family parquet (overwrite mode on)."
      )
      System.exit(1)
    }

    val source = args(0)
    val target = args(1)

    val chunk_size: Int = if (args.length >= 3) {
      args(2).toInt
    } else default_chunk_size

    println("Computing all strict gammoids from path structures")
    println(s"  - loaded from ${source} (parquet)")
    println(
      s"  - writing result to ${target}/ID_from_XXX_to_XXX (parquet, overwrite)"
    )
    println(s"  - in chunks of ${chunk_size} digraphs per run")

    val conf = Spark.spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    Range(0, max_dipath_id + 1, chunk_size).foreach((id0: Int) ⇒ {

      val target_chunk = f"${target}/ID_from_${id0}_to_${id0 + chunk_size - 1}"

      if (
        (!target_chunk.startsWith("gs://")) &&
        fs.exists(new org.apache.hadoop.fs.Path(f"${target_chunk}/_SUCCESS"))
      )
        println(
          s"Skipping ${target_chunk} because it has been successfully created."
        )
      else
        ComputeStrictGammoids.main(
          Array(
            source,
            target_chunk,
            f"${id0}",
            f"${id0 + chunk_size}"
          )
        )
    })

  }

}
