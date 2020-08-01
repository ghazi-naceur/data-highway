package io.oss.data.highway

import com.typesafe.scalalogging.StrictLogging
import io.oss.data.highway.configuration.ConfLoader
import io.oss.data.highway.converter.{
  CsvConverter,
  JsonConverter,
  ParquetConverter
}
import io.oss.data.highway.model._
import io.oss.data.highway.utils.Constants.SEPARATOR
import org.apache.spark.sql.SaveMode.Overwrite

object App extends StrictLogging {

  def main(args: Array[String]): Unit = {
    val result = for {
      conf <- ConfLoader.loadConf()
      _ <- conf match {
        case route @ CsvToParquet(in, out) =>
          ParquetConverter.apply(in, out, SEPARATOR, route.channel, Overwrite)
        case route @ JsonToParquet(in, out) =>
          ParquetConverter.apply(in, out, SEPARATOR, route.channel, Overwrite)
        case route @ XlsxToCsv(in, out) =>
          CsvConverter.apply(in, out, SEPARATOR, route.channel, Overwrite)
        case route @ ParquetToCsv(in, out) =>
          CsvConverter.apply(in, out, SEPARATOR, route.channel, Overwrite)
        case route @ JsonToCsv(in, out) =>
          CsvConverter.apply(in, out, SEPARATOR, route.channel, Overwrite)
        case route @ ParquetToJson(in, out) =>
          JsonConverter.apply(in, out, SEPARATOR, route.channel, Overwrite)
        case route @ CsvToJson(in, out) =>
          JsonConverter.apply(in, out, SEPARATOR, route.channel, Overwrite)
        case _ =>
          throw new RuntimeException(
            s"The provided route '$conf' is ont supported.")
      }
    } yield ()
    result match {
      case Left(thr)    => logger.error("Error", thr.asString)
      case Right(value) => logger.info("Success", value)
    }
  }
}
