name := "clustering-metrics-example"

version := "0.1"
scalaVersion := "2.12.13"

val sparkVersion = "3.2.0"
val deltaVersion = "2.0.0"

lazy val root = (project in file("."))
  .settings(
    Compile / unmanagedJars += file("lib/lighthouse_2.12-0.1.0.jar")
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "io.delta" %% "delta-core" % deltaVersion