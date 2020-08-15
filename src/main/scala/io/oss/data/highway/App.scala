package io.oss.data.highway

import io.oss.data.highway.configuration.ConfLoader
import io.oss.data.highway.converter.{CsvSink, JsonSink, KafkaSink, ParquetSink}
import io.oss.data.highway.model._
import io.oss.data.highway.utils.Constants.SEPARATOR
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.log4j.{BasicConfigurator, Logger}

object App {

  val logger: Logger = Logger.getLogger(classOf[App].getName)

  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    val result = for {
      conf <- ConfLoader.loadConf()
      _ <- conf match {
        case route @ CsvToParquet(in, out) =>
          ParquetSink.apply(in, out, SEPARATOR, route.channel, Overwrite)
        case route @ JsonToParquet(in, out) =>
          ParquetSink.apply(in, out, SEPARATOR, route.channel, Overwrite)
        case route @ XlsxToCsv(in, out) =>
          CsvSink.apply(in, out, SEPARATOR, route.channel, Overwrite)
        case route @ ParquetToCsv(in, out) =>
          CsvSink.apply(in, out, SEPARATOR, route.channel, Overwrite)
        case route @ JsonToCsv(in, out) =>
          CsvSink.apply(in, out, SEPARATOR, route.channel, Overwrite)
        case route @ ParquetToJson(in, out) =>
          JsonSink.apply(in, out, SEPARATOR, route.channel, Overwrite)
        case route @ CsvToJson(in, out) =>
          JsonSink.apply(in, out, SEPARATOR, route.channel, Overwrite)
        case JsonToKafka(in, out, brokerUrl, kafkaMode) =>
          new KafkaSink().sendToTopic(in, out, brokerUrl, kafkaMode)
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
