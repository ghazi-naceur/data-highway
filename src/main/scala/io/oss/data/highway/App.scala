package io.oss.data.highway

import com.typesafe.scalalogging.StrictLogging
import io.oss.data.highway.configuration.ConfLoader
import io.oss.data.highway.model._
import io.oss.data.highway.utils.Constants.{
  SEPARATOR,
  XLSX_EXTENSION,
  XLS_EXTENSION
}
import io.oss.data.highway.utils.{
  CsvHandler,
  JsonHandler,
  ParquetHandler,
  XlsxCsvConverter
}
import org.apache.spark.sql.SaveMode.Overwrite

object App extends StrictLogging {

  def main(args: Array[String]): Unit = {
    val result = for {
      conf <- ConfLoader.loadConf()
      _ <- conf match {
        case XlsxToCsv(in, out) =>
          XlsxCsvConverter.apply(in, out, Seq(XLS_EXTENSION, XLSX_EXTENSION))
        case obj @ CsvToParquet(in, out) =>
          ParquetHandler.apply(in, out, SEPARATOR, obj.value, Overwrite)
        case obj @ JsonToParquet(in, out) =>
          ParquetHandler.apply(in, out, SEPARATOR, obj.value, Overwrite)
        case ParquetToCsv(in, out) =>
          CsvHandler.apply(in, out, SEPARATOR, Overwrite)
        case obj @ ParquetToJson(in, out) =>
          JsonHandler.apply(in, out, SEPARATOR, obj.value, Overwrite)
        case obj @ CsvToJson(in, out) =>
          JsonHandler.apply(in, out, SEPARATOR, obj.value, Overwrite)
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
