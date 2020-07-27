package io.oss.data.highway

import io.oss.data.highway.configuration.ConfLoader
import io.oss.data.highway.model.{
  CsvToParquet,
  ParquetToCsv,
  ParquetToJson,
  XlsxToCsv
}
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

object App {

  def main(args: Array[String]): Unit = {
    for {
      conf <- ConfLoader.loadConf()
      _ <- conf.route match {
        case XlsxToCsv(in, out) =>
          XlsxCsvConverter.apply(in, out, Seq(XLS_EXTENSION, XLSX_EXTENSION))
        case CsvToParquet(in, out) =>
          ParquetHandler.apply(in, out, SEPARATOR, Overwrite)
        case ParquetToCsv(in, out) =>
          CsvHandler.apply(in, out, SEPARATOR, Overwrite)
        case ParquetToJson(in, out) =>
          JsonHandler.apply(in, out, Overwrite)
      }
    } yield ()
  }
}
