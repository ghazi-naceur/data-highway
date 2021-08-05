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
      case CsvToParquet(in, out, storage) =>
        ParquetSink.handleParquetChannel(in, out, Overwrite, storage, CSV)
      case JsonToParquet(in, out, storage) =>
        ParquetSink.handleParquetChannel(in, out, Overwrite, storage, JSON)
      case AvroToParquet(in, out, storage) =>
        ParquetSink.handleParquetChannel(in, out, Overwrite, storage, AVRO)
      case XlsxToCsv(in, out, storage) =>
        CsvSink.handleCsvChannel(in, out, Overwrite, storage, XLSX)
      case ParquetToCsv(in, out, storage) =>
        CsvSink.handleCsvChannel(in, out, Overwrite, storage, PARQUET)
      case AvroToCsv(in, out, storage) =>
        CsvSink.handleCsvChannel(in, out, Overwrite, storage, AVRO)
      case JsonToCsv(in, out, storage) =>
        CsvSink.handleCsvChannel(in, out, Overwrite, storage, JSON)
      case ParquetToJson(in, out, storage) =>
        JsonSink.handleJsonChannel(in, out, Overwrite, storage, PARQUET)
      case AvroToJson(in, out, storage) =>
        JsonSink.handleJsonChannel(in, out, Overwrite, storage, AVRO)
      case CsvToJson(in, out, storage) =>
        JsonSink.handleJsonChannel(in, out, Overwrite, storage, CSV)
      case ParquetToAvro(in, out, storage) =>
        AvroSink.handleAvroChannel(in, out, Overwrite, storage, PARQUET)
      case JsonToAvro(in, out, storage) =>
        AvroSink.handleAvroChannel(in, out, Overwrite, storage, JSON)
      case CsvToAvro(in, out, storage) =>
        AvroSink.handleAvroChannel(in, out, Overwrite, storage, CSV)
      case KafkaToFile(in, out, storage, kafkaMode) =>
        KafkaSampler.consumeFromTopic(in, out, storage, kafkaMode)
      case FileToKafka(in, out, storage, kafkaMode) =>
        KafkaSink.publishToTopic(in, out, storage, kafkaMode)
      case KafkaToKafka(in, out, kafkaMode) =>
        // todo split publishToTopic between FTK and KTK, and omit Local
        KafkaSink.publishToTopic(in, out, Local, kafkaMode)
      case FileToElasticsearch(in, out, storage, bulkEnabled) =>
        ElasticSink.handleElasticsearchChannel(in, out, storage, bulkEnabled)
      case ElasticsearchToFile(in, out, storage, searchQuery) =>
        ElasticSampler.saveDocuments(in, out, storage, searchQuery)
      case ElasticOps(operation) =>
        ElasticAdminOps.execute(operation)
      case FileToCassandra(in, cassandra, storage, dataType) =>
        CassandraSink.handleCassandraChannel(in, cassandra, Append, storage, dataType)
      case _ =>
        throw new RuntimeException(s"The provided route '$route' is not supported.")
    }
  }
}
