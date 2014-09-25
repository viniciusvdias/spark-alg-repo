name := "fpgrowth-scala"

version := "1.0"

scalaVersion := "2.10.4"

scalacOptions += "-deprecation"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies += "org.scalatest" %% "scalatest" % "1.9.1" % "test"

libraryDependencies += "junit" % "junit" % "4.10" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.2"

//libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.0.0-cdh4.7.0"

