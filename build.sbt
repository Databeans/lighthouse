
name := "lighthouse"

version := "0.1.0"

scalaVersion := "2.12.13"

val scalaTestVersion = "3.1.1"
val sparkVersion = "3.3.2"
val deltaVersion = "2.3.0"

libraryDependencies += "org.scalactic" %% "scalactic" % scalaTestVersion
libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion % "test"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "io.delta" %% "delta-core" % deltaVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests"
libraryDependencies += "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests"
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % Test classifier "tests"