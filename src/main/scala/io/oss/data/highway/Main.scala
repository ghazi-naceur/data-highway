package io.oss.data.highway

import io.oss.data.highway.configs.ConfigLoader
import io.oss.data.highway.sinks.{
  AvroSink,
  CassandraSink,
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
import org.apache.spark.sql.SaveMode.{Append, Overwrite}
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
      case KafkaToFile(in, out, fileSystem, kafkaMode) =>
        KafkaSampler.consumeFromTopic(in, out, fileSystem, kafkaMode)
      case FileToKafka(in, out, fileSystem, kafkaMode) =>
        KafkaSink.publishToTopic(in, out, fileSystem, kafkaMode)
      case KafkaToKafka(in, out, kafkaMode) =>
        // todo split publishToTopic between FTK and KTK, and omit Local
        KafkaSink.publishToTopic(in, out, Local, kafkaMode)
      case FileToElasticsearch(in, out, fileSystem, bulkEnabled) =>
        ElasticSink.handleElasticsearchChannel(in, out, fileSystem, bulkEnabled)
      case ElasticsearchToFile(in, out, fileSystem, searchQuery) =>
        ElasticSampler.saveDocuments(in, out, fileSystem, searchQuery)
      case ElasticOps(operation) =>
        ElasticAdminOps.execute(operation)
      case FileToCassandra(in, cassandra, storage, dataType) =>
        CassandraSink.handleCassandraChannel(in, cassandra, Append, storage, dataType)
      case _ =>
        throw new RuntimeException(s"The provided route '$route' is not supported.")
    }
  }
}
