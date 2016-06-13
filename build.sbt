name := "spark-alg-repo"

version := "1.0"

scalaVersion := "2.10.4"

scalacOptions += "-deprecation"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += Resolver.sonatypeRepo("public")

//libraryDependencies += "com.esotericsoftware.kryo" % "kryo" % "3.0.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.1"

libraryDependencies += "com.twitter" % "algebird-core_2.10" % "0.11.0"

libraryDependencies += "com.twitter" % "algebird-util_2.10" % "0.11.0"
