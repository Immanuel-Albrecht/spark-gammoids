/* use assembly to package a uber jar */
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10")

/* JaCoCo Code Coverage */
addSbtPlugin("com.github.sbt" % "sbt-jacoco" % "3.1.0")

/* add spark support */
addSbtPlugin("com.github.alonsodomin" % "sbt-spark" % "0.6.0")


/* support partial unification for scala < 2.13 */
addSbtPlugin("org.lyranthe.sbt" % "partial-unification" % "1.1.2")