package io.oss.data.highway

import io.oss.data.highway.configs.ConfigLoader
import io.oss.data.highway.sinks.{
  AvroSink,
  CsvSink,
  ElasticAdminOps,
  ElasticSampler,
  ElasticSink,
  JsonSink,
  KafkaSampler,
  KafkaSink,
  ParquetSink
}
import io.oss.data.highway.models._
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.log4j.{BasicConfigurator, Logger}

object Main {

  val logger: Logger = Logger.getLogger(Main.getClass.getName)

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

  def apply(route: Route): Either[Throwable, Any] = {
    logger.info(s"${route.toString} route is activated ...")
    route match {
      case CsvToParquet(in, out, fileSystem) =>
        ParquetSink.handleParquetChannel(in, out, Overwrite, fileSystem, CSV)
      case JsonToParquet(in, out, fileSystem) =>
        ParquetSink.handleParquetChannel(in, out, Overwrite, fileSystem, JSON)
      case AvroToParquet(in, out, fileSystem) =>
        ParquetSink.handleParquetChannel(in, out, Overwrite, fileSystem, AVRO)
      case XlsxToCsv(in, out, fileSystem) =>
        CsvSink.handleCsvChannel(in, out, Overwrite, fileSystem, XLSX)
      case ParquetToCsv(in, out, fileSystem) =>
        CsvSink.handleCsvChannel(in, out, Overwrite, fileSystem, PARQUET)
      case AvroToCsv(in, out, fileSystem) =>
        CsvSink.handleCsvChannel(in, out, Overwrite, fileSystem, AVRO)
      case JsonToCsv(in, out, fileSystem) =>
        CsvSink.handleCsvChannel(in, out, Overwrite, fileSystem, JSON)
      case ParquetToJson(in, out, fileSystem) =>
        JsonSink.handleJsonChannel(in, out, Overwrite, fileSystem, PARQUET)
      case AvroToJson(in, out, fileSystem) =>
        JsonSink.handleJsonChannel(in, out, Overwrite, fileSystem, AVRO)
      case CsvToJson(in, out, fileSystem) =>
        JsonSink.handleJsonChannel(in, out, Overwrite, fileSystem, CSV)
      case ParquetToAvro(in, out, fileSystem) =>
        AvroSink.handleAvroChannel(in, out, Overwrite, fileSystem, PARQUET)
      case JsonToAvro(in, out, fileSystem) =>
        AvroSink.handleAvroChannel(in, out, Overwrite, fileSystem, JSON)
      case CsvToAvro(in, out, fileSystem) =>
        AvroSink.handleAvroChannel(in, out, Overwrite, fileSystem, CSV)
      case KafkaToFile(in, out, kafkaMode) =>
        KafkaSampler.consumeFromTopic(in, out, kafkaMode)
      case FileToKafka(in, out, fileSystem, kafkaMode) =>
        KafkaSink.publishToTopic(in, out, fileSystem, kafkaMode)
      case KafkaToKafka(in, out, fileSystem, kafkaMode) =>
        KafkaSink.publishToTopic(in, out, fileSystem, kafkaMode)
      case FileToElasticsearch(in, out, bulkEnabled) =>
        ElasticSink.handleElasticsearchChannel(in, out, bulkEnabled)
      case ElasticsearchToFile(in, out, searchQuery) =>
        ElasticSampler.saveDocuments(in, out, searchQuery)
      case ElasticOps(operation) =>
        ElasticAdminOps.execute(operation)
      case _ =>
        throw new RuntimeException(s"The provided route '$route' is not supported.")
    }
  }
}
