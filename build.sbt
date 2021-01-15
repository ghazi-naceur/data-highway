name := "data-highway"

version := "0.1"

scalaVersion := "2.12.12"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF","services",xs @ _*) => MergeStrategy.filterDistinctLines // To add Spark datasources : org.apache.spark.sql.sources.DataSourceRegister
    // See : https://stackoverflow.com/questions/62232209/classnotfoundexception-caused-by-java-lang-classnotfoundexception-csv-default
  case PathList("META-INF",xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

addCompilerPlugin(
  "org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full
)

val http4sVersion = "0.21.12"
val poiVersion = "4.1.2"
val scalatestVersion = "3.2.0"
val catsVersion = "2.1.1"
val sparkVersion = "2.4.6"
val kafkaVersion = "2.4.0"
val circeVersion = "0.13.0"

libraryDependencies ++= Seq("com.github.pureconfig" %% "pureconfig" % "0.13.0",
  "org.apache.poi" % "poi" % poiVersion,
  "org.apache.poi" % "poi-ooxml" % poiVersion,
  "org.scalatest" %% "scalatest" % scalatestVersion,
  "org.scalatest" %% "scalatest" % scalatestVersion % "test",
  "org.typelevel" %% "cats-core" % catsVersion,
  "org.typelevel" %% "cats-effect" % catsVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark" %% "spark-sql" % sparkVersion exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark" %% "spark-hive" % sparkVersion exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark" %% "spark-avro" % sparkVersion exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark" %% "spark-streaming" % sparkVersion exclude("org.slf4j", "slf4j-log4j12"),
  "MrPowers" % "spark-fast-tests" % "0.20.0-s_2.12" exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.hadoop" % "hadoop-client" % "3.1.3",
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % kafkaVersion,
  "net.manub" %% "scalatest-embedded-kafka" % "2.0.0" % "test",
  "log4j" % "log4j" % "1.2.17",
  "org.slf4j" % "slf4j-log4j12" % "1.7.25",
  "commons-logging" % "commons-logging" % "1.2",
  "org.http4s" %% "http4s-dsl" % http4sVersion,
  "org.http4s" %% "http4s-blaze-server" % http4sVersion,
  "org.http4s" %% "http4s-blaze-client" % http4sVersion,
  "org.http4s" %% "http4s-circe" % http4sVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.monix" %% "monix" % "3.3.0"
)

scalacOptions += "-Ypartial-unification"
parallelExecution in Test := false