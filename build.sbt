name := "spark-gammoids"

version := "0.1"

scalaVersion := "2.12.8"

resolvers += Resolver.bintrayIvyRepo("com.eed3si9n", "sbt-plugins")
val sparkVersion = "2.4.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.commons" % "commons-csv" % "1.2"


libraryDependencies += "org.scala-graph" %% "graph-core" % "1.12.5"
libraryDependencies += "org.jgrapht" % "jgrapht-core" % "1.4.0"