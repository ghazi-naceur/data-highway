name := "data-highway"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq("com.github.pureconfig" %% "pureconfig" % "0.13.0",
  "org.apache.poi" % "poi" % "4.1.2",
  "org.apache.poi" % "poi-ooxml" % "4.1.2",
  "org.scalatest" %% "scalatest" % "3.2.0",
  "org.scalatest" %% "scalatest" % "3.2.0" % "test",
  "org.typelevel" %% "cats-core" % "2.1.1",
  "org.typelevel" %% "cats-effect" % "2.1.1",
  "org.apache.spark" %% "spark-core" % "2.4.6",
  "org.apache.spark" %% "spark-sql" % "2.4.6",
  "org.apache.spark" %% "spark-hive" % "2.4.6",
  "org.apache.spark" %% "spark-avro" % "2.4.6",
  "org.apache.spark" %% "spark-streaming" % "2.4.6"
)

scalacOptions += "-Ypartial-unification"