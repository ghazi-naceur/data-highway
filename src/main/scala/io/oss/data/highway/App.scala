package io.oss.data.highway

import io.oss.data.highway.configuration.ConfigLoader
import io.oss.data.highway.converter.{
  AvroSink,
  CsvSink,
  JsonSink,
  KafkaSink,
  ParquetSink
}
import io.oss.data.highway.model._
import io.oss.data.highway.utils.Constants.{XLSX_EXTENSION, XLS_EXTENSION}
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.log4j.{BasicConfigurator, Logger}

object App {

  val logger: Logger = Logger.getLogger(classOf[App].getName)

  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    val result = for {
      conf <- ConfigLoader().loadConf()
      sparkConfig <- ConfigLoader().loadSparkConf()
      _ <- conf match {
        case CsvToParquet(in, out) =>
          ParquetSink.handleParquetChannel(in, out, Overwrite, CSV, sparkConfig)
        case JsonToParquet(in, out) =>
          ParquetSink.handleParquetChannel(in,
                                           out,
                                           Overwrite,
                                           JSON,
                                           sparkConfig)
        case AvroToParquet(in, out) =>
          ParquetSink.handleParquetChannel(in,
                                           out,
                                           Overwrite,
                                           PARQUET,
                                           sparkConfig)
        case XlsxToCsv(in, out) =>
          CsvSink.handleXlsxCsvChannel(in,
                                       out,
                                       Seq(XLSX_EXTENSION, XLS_EXTENSION))
        case ParquetToCsv(in, out) =>
          CsvSink.handleCsvChannel(in, out, Overwrite, PARQUET, sparkConfig)
        case AvroToCsv(in, out) =>
          CsvSink.handleCsvChannel(in, out, Overwrite, AVRO, sparkConfig)
        case JsonToCsv(in, out) =>
          CsvSink.handleCsvChannel(in, out, Overwrite, JSON, sparkConfig)
        case ParquetToJson(in, out) =>
          JsonSink.handleJsonChannel(in, out, Overwrite, PARQUET, sparkConfig)
        case AvroToJson(in, out) =>
          JsonSink.handleJsonChannel(in, out, Overwrite, JSON, sparkConfig)
        case CsvToJson(in, out) =>
          JsonSink.handleJsonChannel(in, out, Overwrite, CSV, sparkConfig)
        case KafkaToFile(in,
                         out,
                         dataType,
                         brokerUrl,
                         kafkaMode,
                         offset,
                         consumerGroup) =>
          JsonSink.store(in, out, dataType, brokerUrl, offset, consumerGroup)
        case JsonToKafka(in, out, brokerUrl, kafkaMode) =>
          new KafkaSink().sendToTopic(in,
                                      out,
                                      brokerUrl,
                                      kafkaMode,
                                      sparkConfig)
        case ParquetToAvro(in, out) =>
          AvroSink.handleAvroChannel(in, out, Overwrite, PARQUET, sparkConfig)
        case JsonToAvro(in, out) =>
          AvroSink.handleAvroChannel(in, out, Overwrite, JSON, sparkConfig)
        case CsvToAvro(in, out) =>
          AvroSink.handleAvroChannel(in, out, Overwrite, CSV, sparkConfig)
        case _ =>
          throw new RuntimeException(
            s"The provided route '$conf' is ont supported.")
      }
    } yield ()
    result match {
      case Left(thr) =>
        logger.error("Error : " + thr.toString)
      case Right(_) => logger.info("Success")
    }
  }
}
