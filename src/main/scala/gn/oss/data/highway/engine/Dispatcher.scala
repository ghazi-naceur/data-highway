package gn.oss.data.highway.engine

import gn.oss.data.highway.configs.ConfigLoader
import gn.oss.data.highway.models.{
  Cassandra,
  Channel,
  Consistency,
  DataHighwayErrorResponse,
  DataHighwayResponse,
  ElasticOps,
  Elasticsearch,
  File,
  Kafka,
  Output,
  Route,
  Storage
}
import org.apache.log4j.{BasicConfigurator, Logger}
import org.apache.spark.sql.SaveMode.{Append, Overwrite}
import pureconfig.generic.auto._

object Dispatcher {

  val logger: Logger = Logger.getLogger(Dispatcher.getClass.getName)

  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    val route = ConfigLoader().loadConfigs[Route]("route")
    apply(route)
  }

  def apply(route: Channel): Either[DataHighwayErrorResponse, DataHighwayResponse] = {
    logger.info(s"${route.toString} route is activated ...")
    route match {
      case ElasticOps(operation) =>
        ElasticAdminOps.execute(operation)
      case Route(
            input: File,
            output: File,
            storage: Option[Storage],
            saveMode: Option[Consistency]
          ) =>
        BasicSink.handleChannel(input, output, storage, saveMode)
      case Route(
            input: File,
            output: Cassandra,
            storage: Option[Storage],
            saveMode: Option[Consistency]
          ) =>
        CassandraSink
          .handleCassandraChannel(input, output, storage, saveMode)
      case Route(
            input: Cassandra,
            output: Output,
            _: Option[Storage],
            saveMode: Option[Consistency]
          ) =>
        CassandraSampler.extractRows(input, output, saveMode)
      case Route(
            input: File,
            output: Elasticsearch,
            storage: Option[Storage],
            saveMode: Option[Consistency]
          ) =>
        ElasticSink.handleElasticsearchChannel(input, output, storage)
      case Route(
            input: Elasticsearch,
            output: Output,
            storage: Option[Storage],
            saveMode: Option[Consistency]
          ) =>
        ElasticSampler.saveDocuments(input, output, Overwrite, storage)
      case Route(
            input: File,
            output: Kafka,
            storage: Option[Storage],
            saveMode: Option[Consistency]
          ) =>
        KafkaSink.handleKafkaChannel(input, output, storage)
      case Route(input: Kafka, output: Kafka, _, _) =>
        KafkaSink.mirrorTopic(input, output)
      case Route(
            input: Kafka,
            output: Output,
            storage: Option[Storage],
            saveMode: Option[Consistency]
          ) =>
        KafkaSampler.consumeFromTopic(input, output, Append, storage)
      case _ =>
        throw new RuntimeException(s"""
          | The provided route '$route' is not supported yet. This route will be implemented in the upcoming versions.
          | For now, you can combine all the available routes to ensure sending data to your desired destination.
          |""".stripMargin)
    }
  }
}
