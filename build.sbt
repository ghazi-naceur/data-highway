name := "data-highway"

version := "0.1"

scalaVersion := "2.12.12"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq("com.github.pureconfig" %% "pureconfig" % "0.13.0",
  "org.apache.poi" % "poi" % "4.1.2",
  "org.apache.poi" % "poi-ooxml" % "4.1.2",
  "org.scalatest" %% "scalatest" % "3.2.0",
  "org.scalatest" %% "scalatest" % "3.2.0" % "test",
  "org.typelevel" %% "cats-core" % "2.1.1",
  "org.typelevel" %% "cats-effect" % "2.1.1",
  "org.apache.spark" %% "spark-core" % "2.4.6" exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark" %% "spark-sql" % "2.4.6" exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark" %% "spark-hive" % "2.4.6" exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark" %% "spark-avro" % "2.4.6" exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark" %% "spark-streaming" % "2.4.6" exclude("org.slf4j", "slf4j-log4j12"),
  "MrPowers" % "spark-fast-tests" % "0.20.0-s_2.12" exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.kafka" %% "kafka" % "2.4.0",
  "org.apache.kafka" %% "kafka-streams-scala" % "2.4.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.8.2",
  "org.apache.logging.log4j" % "log4j-api"        % "2.8.2",
  "org.apache.logging.log4j" % "log4j-core"       % "2.8.2"
)

scalacOptions += "-Ypartial-unification"
parallelExecution in Test := false