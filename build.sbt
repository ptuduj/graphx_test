name := "graphx_test"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.5"

mainClass := Some("Main")

