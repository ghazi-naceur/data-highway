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

  def apply(route: RouteBis): Either[Throwable, Any] = {
    logger.info(s"${route.toString} route is activated ...")
    route match {
      case KafkaToFile(in, out, storage, kafkaMode) =>
        KafkaSampler.consumeFromTopic(in, out, storage, kafkaMode)
//      case FileToKafka(in, out, storage, kafkaMode) =>
//        KafkaSink.publishFilesContentToTopic(in, out, storage, kafkaMode)
//      case KafkaToKafka(in, out, kafkaMode) =>
//         todo split publishToTopic between FTK and KTK, and omit Local
//        KafkaSink.publishFilesContentToTopic(in, out, Local, kafkaMode)
      case ElasticOps(operation) =>
        ElasticAdminOps.execute(operation)
      case Route(input: File, output: File, storage: Option[Storage]) =>
        BasicSink.handleChannel(input, output, storage, Overwrite)
      case Route(input: File, output: Cassandra, storage: Option[Storage]) =>
        CassandraSink.handleCassandraChannel(input, output, storage, Append)
      case Route(input: Cassandra, output: File, _) =>
        CassandraSampler.extractRows(input, output.dataType, output.path, Append)
      case Route(input: File, output: Elasticsearch, storage: Option[Storage]) =>
        input.dataType match {
          case JSON =>
            ElasticSink.handleElasticsearchChannel(input, output, storage)
          case _ =>
            // todo Implement all data types support
            Left(new RuntimeException("Only JSON data type is supported."))
        }
      case Route(input: Elasticsearch, output: File, storage: Option[Storage]) =>
        output.dataType match {
          case JSON =>
            ElasticSampler.saveDocuments(input, output, storage)
          case _ =>
            // todo Implement all data types support
            Left(new RuntimeException("Only JSON data type is supported."))
        }
      case Route(input: File, output: Kafka, storage: Option[Storage]) =>
        KafkaSink.publishFilesContentToTopic(input, output, storage)
      case Route(input: Kafka, output: Kafka, storage: Option[Storage]) =>
        KafkaSink.mirrorTopic(input, output)
      case Route(input: Kafka, output: File, storage: Option[Storage]) =>
        Right() // todo only json is supported, make other types supported too
      case _ =>
        throw new RuntimeException(s"The provided route '$route' is not supported.")
    }
  }
}
