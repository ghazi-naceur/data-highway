package io.oss.data.highway

import io.oss.data.highway.configuration.{ConfigLoader, SparkConfigs}
import io.oss.data.highway.converter.{
  AvroSink,
  CsvSink,
  JsonSink,
  KafkaSampler,
  KafkaSink,
  ParquetSink
}
import io.oss.data.highway.model._
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.log4j.{BasicConfigurator, Logger}

object Main {

  val logger: Logger = Logger.getLogger(Main.getClass.getName)

  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    val result = for {
      route <- ConfigLoader().loadConf()
      sparkConf <- ConfigLoader().loadSparkConf()
      _ = logger.info("Successfully loading configurations")
      _ <- apply(sparkConf, route)
    } yield ()
    result match {
      case Left(thr) =>
        logger.error(s"Error : ${thr.toString}")
      case Right(_) => logger.info("Started successfully")
    }
  }

  def apply(sparkConf: SparkConfigs, route: Route): Either[Throwable, Any] = {
    route match {
      case CsvToParquet(in, out) =>
        ParquetSink.handleParquetChannel(in, out, Overwrite, CSV, sparkConf)
      case JsonToParquet(in, out) =>
        ParquetSink.handleParquetChannel(in, out, Overwrite, JSON, sparkConf)
      case AvroToParquet(in, out) =>
        ParquetSink.handleParquetChannel(in, out, Overwrite, AVRO, sparkConf)
      case XlsxToCsv(in, out) =>
        CsvSink.handleXlsxCsvChannel(in,
                                     out,
                                     Seq(XLSX.extension, XLS.extension))
      case ParquetToCsv(in, out) =>
        CsvSink.handleCsvChannel(in, out, Overwrite, PARQUET, sparkConf)
      case AvroToCsv(in, out) =>
        CsvSink.handleCsvChannel(in, out, Overwrite, AVRO, sparkConf)
      case JsonToCsv(in, out) =>
        CsvSink.handleCsvChannel(in, out, Overwrite, JSON, sparkConf)
      case ParquetToJson(in, out) =>
        JsonSink.handleJsonChannel(in, out, Overwrite, PARQUET, sparkConf)
      case AvroToJson(in, out) =>
        JsonSink.handleJsonChannel(in, out, Overwrite, AVRO, sparkConf)
      case CsvToJson(in, out) =>
        JsonSink.handleJsonChannel(in, out, Overwrite, CSV, sparkConf)
      case ParquetToAvro(in, out) =>
        AvroSink.handleAvroChannel(in, out, Overwrite, PARQUET, sparkConf)
      case JsonToAvro(in, out) =>
        AvroSink.handleAvroChannel(in, out, Overwrite, JSON, sparkConf)
      case CsvToAvro(in, out) =>
        AvroSink.handleAvroChannel(in, out, Overwrite, CSV, sparkConf)
      case KafkaToFile(in,
                       out,
                       dataType,
                       brokers,
                       kafkaMode,
                       offset,
                       consGroup) =>
        KafkaSampler.consumeFromTopic(in,
                                      out,
                                      dataType,
                                      kafkaMode,
                                      brokers,
                                      offset,
                                      consGroup,
                                      sparkConf)
      case JsonToKafka(in, out, brokerUrl, kafkaMode) =>
        new KafkaSink().publishToTopic(in, out, brokerUrl, kafkaMode, sparkConf)
      case _ =>
        throw new RuntimeException(
          s"The provided route '$route' is not supported.")
    }
  }
}