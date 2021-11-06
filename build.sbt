name := "data-highway"

version := "0.5-rc"

scalaVersion := "2.12.12"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/release"

lazy val root = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "gn.oss.data.highway.build.info"
  )

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

test in assembly := {}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "services", xs @ _*) =>
    MergeStrategy.concat // To add Spark data sources : org.apache.spark.sql.sources.DataSourceRegister
  // See : https://stackoverflow.com/questions/62232209/classnotfoundexception-caused-by-java-lang-classnotfoundexception-csv-default
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _                             => MergeStrategy.first
}

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)

val catsVersion = "2.1.1"
val scalatestVersion = "3.2.0"
val pureconfigVersion = "0.13.0"
val monixVersion = "3.3.0"
val circeVersion = "0.13.0"
val http4sVersion = "0.21.12"
val sparkVersion = "2.4.6"
val sparkExcelVersion = "0.13.7"
val sparkConnectorVersion = "2.5.0"
val sparkFastTestsVersion = "0.23.0"
val kafkaVersion = "2.4.0"
val elastic4sVersion = "7.10.2"
val postgresVersion = "42.2.24"
val hadoopVersion = "3.3.0"
val logbackVersion = "1.2.3"
val scalaLoggingVersion = "3.9.4"

libraryDependencies ++= Seq(
  "com.github.pureconfig" %% "pureconfig" % pureconfigVersion,
  "org.scalatest" %% "scalatest" % scalatestVersion,
  "org.scalatest" %% "scalatest" % scalatestVersion % Test,
  "org.typelevel" %% "cats-core" % catsVersion,
  "org.typelevel" %% "cats-effect" % catsVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion exclude ("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark" %% "spark-sql" % sparkVersion exclude ("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark" %% "spark-hive" % sparkVersion exclude ("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark" %% "spark-avro" % sparkVersion exclude ("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark" %% "spark-streaming" % sparkVersion exclude ("org.slf4j", "slf4j-log4j12"),
  "com.crealytics" %% "spark-excel" % sparkExcelVersion exclude ("org.slf4j", "slf4j-log4j12"),
  "com.datastax.spark" %% "spark-cassandra-connector" % sparkConnectorVersion exclude ("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark" %% "spark-sql-kafka-0-10" % kafkaVersion exclude ("org.slf4j", "slf4j-log4j12"),
  "com.github.mrpowers" %% "spark-fast-tests" % sparkFastTestsVersion % Test exclude ("org.slf4j", "slf4j-log4j12"),
  "org.apache.hadoop" % "hadoop-hdfs-client" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-minicluster" % hadoopVersion % Test,
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
  "io.github.embeddedkafka" %% "embedded-kafka" % kafkaVersion % Test,
  "ch.qos.logback" % "logback-classic" % logbackVersion,
  "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
  "org.http4s" %% "http4s-dsl" % http4sVersion,
  "org.http4s" %% "http4s-blaze-server" % http4sVersion,
  "org.http4s" %% "http4s-blaze-client" % http4sVersion,
  "org.http4s" %% "http4s-circe" % http4sVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.monix" %% "monix" % monixVersion,
  "com.sksamuel.elastic4s" %% "elastic4s-client-esjava" % elastic4sVersion,
  "org.postgresql" % "postgresql" % postgresVersion
)

scalacOptions += "-Ypartial-unification"
parallelExecution in Test := false
