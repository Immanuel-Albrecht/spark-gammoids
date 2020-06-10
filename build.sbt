name := "spark-gammoids"

version := "0.1"

scalaVersion := "2.12.8"

resolvers += Resolver.bintrayIvyRepo("com.eed3si9n", "sbt-plugins")
resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"
resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"


val sparkVersion = "2.4.5"

// Spark
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.commons" % "commons-csv" % "1.2"

// Unit Tests
libraryDependencies += "org.scalactic" %% "scalactic" % "3.1.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.2" % "test"


// A bunch of libraries that may become handy when handling graphs
libraryDependencies += "org.scala-graph" %% "graph-core" % "1.12.5"
libraryDependencies += "org.jgrapht" % "jgrapht-core" % "1.4.0"
libraryDependencies += "org.jgrapht" % "jgrapht-ext" % "1.4.0"


// Chemistry Development Kit: Chemists love graph isomorphism problems, too
// https://mvnrepository.com/artifact/org.openscience.cdk/cdk-bundle
libraryDependencies += "org.openscience.cdk" % "cdk-bundle" % "2.3"

assemblyMergeStrategy in assembly := {
  case "log4j.properties"                          => MergeStrategy.concat
  case "header.txt"                                => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}