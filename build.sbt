
name := "Assignments"

version := "0.1"

scalaVersion := "2.10.5"

val sparkVersion = "1.5.2"

// Force Scala Version
ivyScala := ivyScala.value map {
  _.copy(overrideScalaVersion = true)
}

// Resolvers
resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"
resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"
resolvers += "Conjars Repository" at "http://conjars.org/repo/"
resolvers += "jitpack" at "https://jitpack.io"

// Spark Library
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion excludeAll
  ExclusionRule(organization = "org.eclipse.jetty")
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

// Source and Sink Library
libraryDependencies += "com.databricks" %% "spark-csv" % "1.3.0"

// Test Setting
parallelExecution in Test := false