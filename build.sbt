name := "spark-alg-repo"

version := "1.0"

scalaVersion := "2.10.4"

scalacOptions += "-deprecation"

parallelExecution in Test := false

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += Resolver.sonatypeRepo("public")

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.1"

libraryDependencies += "com.twitter" % "algebird-core_2.10" % "0.11.0"

libraryDependencies += "com.twitter" % "algebird-util_2.10" % "0.11.0"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.3.0"
