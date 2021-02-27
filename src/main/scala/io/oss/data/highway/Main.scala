package io.oss.data.highway

import io.oss.data.highway.configs.ConfigLoader
import io.oss.data.highway.sinks.{
  AvroSink,
  CsvSink,
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
      case CsvToParquet(in, out) =>
        ParquetSink.handleParquetChannel(in, out, Overwrite, CSV)
      case JsonToParquet(in, out) =>
        ParquetSink.handleParquetChannel(in, out, Overwrite, JSON)
      case AvroToParquet(in, out) =>
        ParquetSink.handleParquetChannel(in, out, Overwrite, AVRO)
      case XlsxToCsv(in, out) =>
        CsvSink.handleXlsxCsvChannel(in,
                                     out,
                                     Seq(XLSX.extension, XLS.extension))
      case ParquetToCsv(in, out) =>
        CsvSink.handleCsvChannel(in, out, Overwrite, PARQUET)
      case AvroToCsv(in, out) =>
        CsvSink.handleCsvChannel(in, out, Overwrite, AVRO)
      case JsonToCsv(in, out) =>
        CsvSink.handleCsvChannel(in, out, Overwrite, JSON)
      case ParquetToJson(in, out) =>
        JsonSink.handleJsonChannel(in, out, Overwrite, PARQUET)
      case AvroToJson(in, out) =>
        JsonSink.handleJsonChannel(in, out, Overwrite, AVRO)
      case CsvToJson(in, out) =>
        JsonSink.handleJsonChannel(in, out, Overwrite, CSV)
      case ParquetToAvro(in, out) =>
        AvroSink.handleAvroChannel(in, out, Overwrite, PARQUET)
      case JsonToAvro(in, out) =>
        AvroSink.handleAvroChannel(in, out, Overwrite, JSON)
      case CsvToAvro(in, out) =>
        AvroSink.handleAvroChannel(in, out, Overwrite, CSV)
      case KafkaToFile(in, out, kafkaMode) =>
        KafkaSampler.consumeFromTopic(in, out, kafkaMode)
      case FileToKafka(in, out, kafkaMode) =>
        new KafkaSink().publishToTopic(in, out, kafkaMode)
      case KafkaToKafka(in, out, kafkaMode) =>
        new KafkaSink().publishToTopic(in, out, kafkaMode)
      case FileToElasticsearch(in, out) =>
        ElasticSink.handleElasticsearchChannel(in, out)
      case ElasticsearchToFile(in, out, searchQuery) =>
        ElasticSampler.saveDocuments(in, out, searchQuery)
      case _ =>
        throw new RuntimeException(
          s"The provided route '$route' is not supported.")
    }
  }
}
