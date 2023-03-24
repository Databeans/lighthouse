
name := "ClusteringInfo"

version := "0.1.1"

scalaVersion := "2.12.13"

val scalaTestVersion = "3.1.1"
val sparkVersion = "3.2.0"
val deltaVersion = "2.0.0"

// unmanagedBase := new java.io.File("/opt/miniconda3/lib/python3.9/site-packages/pyspark/jars")
libraryDependencies += "org.scalactic" %% "scalactic" % scalaTestVersion
libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion % "test"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "io.delta" %% "delta-core" % deltaVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests"
libraryDependencies += "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests"
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % Test classifier "tests"

