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
        case route @ CsvToParquet(in, out) =>
          ParquetSink.apply(in, out, route.channel, Overwrite, sparkConfig)
        case route @ JsonToParquet(in, out) =>
          ParquetSink.apply(in, out, route.channel, Overwrite, sparkConfig)
        case route @ XlsxToCsv(in, out) =>
          CsvSink.apply(in, out, route.channel, Overwrite, sparkConfig)
        case route @ ParquetToCsv(in, out) =>
          CsvSink.apply(in, out, route.channel, Overwrite, sparkConfig)
        case route @ JsonToCsv(in, out) =>
          CsvSink.apply(in, out, route.channel, Overwrite, sparkConfig)
        case route @ ParquetToJson(in, out) =>
          JsonSink.apply(in, out, route.channel, Overwrite, sparkConfig)
        case route @ CsvToJson(in, out) =>
          JsonSink.apply(in, out, route.channel, Overwrite, sparkConfig)
        case JsonToKafka(in, out, brokerUrl, kafkaMode) =>
          new KafkaSink().sendToTopic(in,
                                      out,
                                      brokerUrl,
                                      kafkaMode,
                                      sparkConfig)
        case route @ ParquetToAvro(in, out) =>
          AvroSink.apply(in, out, route.channel, Overwrite, sparkConfig)
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
