name := "data-highway"

version := "0.4"

scalaVersion := "2.12.12"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/release"

lazy val root = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "io.oss.data.highway.build.info"
  )

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

test in assembly := {}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "services", xs @ _*) =>
    MergeStrategy.concat // To add Spark datasources : org.apache.spark.sql.sources.DataSourceRegister
  // See : https://stackoverflow.com/questions/62232209/classnotfoundexception-caused-by-java-lang-classnotfoundexception-csv-default
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _                             => MergeStrategy.first
}

addCompilerPlugin(
  "org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full
)

val http4sVersion    = "0.21.12"
val scalatestVersion = "3.2.0"
val catsVersion      = "2.1.1"
val sparkVersion     = "2.4.6"
val kafkaVersion     = "2.4.0"
val circeVersion     = "0.13.0"
val elastic4sVersion = "7.10.2"
val hadoopVersion    = "3.3.0"

libraryDependencies ++= Seq(
  "com.github.pureconfig"   %% "pureconfig"                % "0.13.0",
  "org.scalatest"           %% "scalatest"                 % scalatestVersion,
  "org.scalatest"           %% "scalatest"                 % scalatestVersion % Test,
  "org.typelevel"           %% "cats-core"                 % catsVersion,
  "org.typelevel"           %% "cats-effect"               % catsVersion,
  "org.apache.spark"        %% "spark-core"                % sparkVersion exclude ("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark"        %% "spark-sql"                 % sparkVersion exclude ("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark"        %% "spark-hive"                % sparkVersion exclude ("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark"        %% "spark-avro"                % sparkVersion exclude ("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark"        %% "spark-streaming"           % sparkVersion exclude ("org.slf4j", "slf4j-log4j12"),
  "com.datastax.spark"      %% "spark-cassandra-connector" % "2.5.0",
  "com.github.mrpowers"     %% "spark-fast-tests"          % "0.23.0"         % Test,
  "org.apache.hadoop"        % "hadoop-hdfs-client"        % hadoopVersion,
  "org.apache.hadoop"        % "hadoop-common"             % hadoopVersion,
  "org.apache.hadoop"        % "hadoop-minicluster"        % hadoopVersion    % Test,
  "org.apache.kafka"        %% "kafka"                     % kafkaVersion,
  "org.apache.kafka"        %% "kafka-streams-scala"       % kafkaVersion,
  "org.apache.spark"        %% "spark-sql-kafka-0-10"      % kafkaVersion,
  "io.github.embeddedkafka" %% "embedded-kafka"            % kafkaVersion     % Test,
  "org.slf4j"                % "slf4j-log4j12"             % "1.7.25",
  "commons-logging"          % "commons-logging"           % "1.2",
  "org.http4s"              %% "http4s-dsl"                % http4sVersion,
  "org.http4s"              %% "http4s-blaze-server"       % http4sVersion,
  "org.http4s"              %% "http4s-blaze-client"       % http4sVersion,
  "org.http4s"              %% "http4s-circe"              % http4sVersion,
  "io.circe"                %% "circe-generic"             % circeVersion,
  "io.monix"                %% "monix"                     % "3.3.0",
  "com.sksamuel.elastic4s"  %% "elastic4s-client-esjava"   % elastic4sVersion,
  "com.crealytics"          %% "spark-excel"               % "0.13.7"
)

scalacOptions += "-Ypartial-unification"
parallelExecution in Test := false
