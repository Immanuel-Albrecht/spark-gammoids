name := "spark-gammoids"

version := "0.1"

scalaVersion := "2.12.8"

resolvers += Resolver.bintrayIvyRepo("com.eed3si9n", "sbt-plugins")
resolvers += "Artima Maven Repository" at "https://repo.artima.com/releases"
resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
resolvers += "central" at "https://repo1.maven.org/maven2/"
resolvers += "spring.io" at "https://repo.spring.io/plugins-release/"

logBuffered in Test := false

enablePlugins(SparkPlugin)

sparkVersion := "2.4.5"

// Spark
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion.value % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion.value % Provided
//libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion.value % Provided
//libraryDependencies +=  "org.apache.derby" % "derby" % "10.4.1.3" % Test
//libraryDependencies += "org.apache.commons" % "commons-csv" % "1.2"
// https://mvnrepository.com/artifact/apache-xerces/xml-apis
//libraryDependencies += "apache-xerces" % "xml-apis" % "2.7.0"

// upgrade for the === operator to obey .Equality[..] via import org.scalactic._
libraryDependencies += "org.scalactic" %% "scalactic" % "3.1.2"
// Unit Tests
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.2" % Test

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oNCXELOPQRM")


// Cats
scalacOptions += "-Ypartial-unification"
libraryDependencies += "org.typelevel" %% "cats-core" % "2.1.1"


// A bunch of libraries that may become handy when handling graphs
//libraryDependencies += "org.scala-graph" %% "graph-core" % "1.12.5"
//libraryDependencies += "org.jgrapht" % "jgrapht-core" % "1.4.0"
//libraryDependencies += "org.jgrapht" % "jgrapht-ext" % "1.4.0"


// Chemistry Development Kit: Chemists love graph isomorphism problems, too
// https://mvnrepository.com/artifact/org.openscience.cdk/cdk-bundle
// libraryDependencies += "org.openscience.cdk" % "cdk-bundle" % "2.3"

/* fix some issues on assembly */

assemblyMergeStrategy in assembly := {
  case "log4j.properties"                          => MergeStrategy.concat
  case "header.txt"                                => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
