package gn.oss.data.highway.engine

import com.typesafe.scalalogging.LazyLogging
import gn.oss.data.highway.configs.ConfigLoader
import gn.oss.data.highway.engine.converter.FileConverter
import gn.oss.data.highway.engine.extractors.{DBConnectorExtractor, ElasticExtractor, KafkaExtractor}
import gn.oss.data.highway.engine.ops.{ElasticAdminOps, KafkaAdminOps}
import gn.oss.data.highway.engine.sinks.{DBConnectorSink, ElasticSink, KafkaSink}
import gn.oss.data.highway.models.DataHighwayRuntimeException.RouteError
import gn.oss.data.highway.models.{
  Channel,
  Consistency,
  DBConnector,
  DataHighwayErrorResponse,
  DataHighwaySuccessResponse,
  ElasticOps,
  Elasticsearch,
  File,
  Kafka,
  KafkaOps,
  Output,
  Route,
  Storage
}
import org.apache.log4j.BasicConfigurator
import pureconfig.generic.auto._

object Dispatcher extends LazyLogging {

  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    val route = ConfigLoader().loadConfigs[Route]("route")
    apply(route)
  }

  def apply(route: Channel): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    logger.info(s"${route.toString} route is activated ...")
    route match {
      case ElasticOps(operation) => ElasticAdminOps.execute(operation)
      case KafkaOps(operation)   => KafkaAdminOps.execute(operation)
      case Route(input: File, output: File, storage: Option[Storage], saveMode: Option[Consistency]) =>
        FileConverter.handleChannel(input, output, storage, saveMode)
      case Route(input: File, output: DBConnector, storage: Option[Storage], saveMode: Option[Consistency]) =>
        DBConnectorSink.handleDBConnectorChannel(input, output, storage, saveMode)
      case Route(input: File, output: Elasticsearch, storage: Option[Storage], _: Option[Consistency]) =>
        ElasticSink.handleElasticsearchChannel(input, output, storage)
      case Route(input: File, output: Kafka, storage: Option[Storage], _: Option[Consistency]) =>
        KafkaSink.handleKafkaChannel(input, output, storage)
      case Route(input: DBConnector, output: Output, _: Option[Storage], saveMode: Option[Consistency]) =>
        DBConnectorExtractor.extractRows(input, output, saveMode)
      case Route(input: Elasticsearch, output: Output, storage: Option[Storage], saveMode: Option[Consistency]) =>
        ElasticExtractor.saveDocuments(input, output, storage, saveMode)
      case Route(input: Kafka, output: Kafka, _: Option[Storage], _: Option[Consistency]) =>
        KafkaSink.mirrorTopic(input, output)
      case Route(input: Kafka, output: Output, storage: Option[Storage], saveMode: Option[Consistency]) =>
        KafkaExtractor.consumeFromTopic(input, output, storage, saveMode)
      case _ => Left(RouteError)
    }
  }
}
