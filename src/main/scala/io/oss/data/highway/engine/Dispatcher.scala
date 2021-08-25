package io.oss.data.highway.engine

import io.oss.data.highway.configs.ConfigLoader
import io.oss.data.highway.models._
import org.apache.log4j.{BasicConfigurator, Logger}
import org.apache.spark.sql.SaveMode.{Append, Overwrite}

object Dispatcher {

  val logger: Logger = Logger.getLogger(Dispatcher.getClass.getName)

  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    val result = for {
      route <- ConfigLoader().loadConf()
      _ = logger.info("Successfully loading configurations")
      _ <- apply(route)
    } yield ()
    result match {
      case Left(thr) =>
        logger.error(s"Error : ${thr.toString}")
      case Right(_) => logger.info("Started successfully")
    }
  }

  def apply(route: Channel): Either[Throwable, Any] = {
    logger.info(s"${route.toString} route is activated ...")
    route match {
      case ElasticOps(operation) =>
        ElasticAdminOps.execute(operation)
      case Route(input: File, output: File, storage: Option[Storage]) =>
        BasicSink.handleChannel(input, output, storage, Overwrite)
      case Route(input: File, output: Cassandra, storage: Option[Storage]) =>
        CassandraSink.handleCassandraChannel(input, output, storage, Append)
      case Route(input: Cassandra, output: File, _) =>
        CassandraSampler.extractRows(input, output.dataType, output.path, Append)
      case Route(input: File, output: Elasticsearch, storage: Option[Storage]) =>
        ElasticSink.handleElasticsearchChannel(input, output, storage)
      case Route(input: Elasticsearch, output: File, storage: Option[Storage]) =>
        ElasticSampler.saveDocuments(input, output, Overwrite, storage)
      case Route(input: File, output: Kafka, storage: Option[Storage]) =>
        KafkaSink.handleKafkaChannel(input, output, storage)
      case Route(input: Kafka, output: Kafka, _) =>
        KafkaSink.mirrorTopic(input, output)
      case Route(input: Kafka, output: File, storage: Option[Storage]) =>
        KafkaSampler.consumeFromTopic(input, output, Append, storage)
      case Route(input: Kafka, output: AdvancedOutput, storage: Option[Storage]) =>
        AdvancedSink.handleRoute(input, output, Append, storage)
      case _ =>
        throw new RuntimeException(s"""
          | The provided route '$route' is not supported yet. This route will be implemented in the upcoming versions.
          | For now, you can combine all the available routes to ensure sending data to your desired destination.
          |""".stripMargin)
    }
  }
}
