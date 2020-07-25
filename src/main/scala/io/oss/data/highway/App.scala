package io.oss.data.highway

import io.oss.data.highway.configuration.ConfLoader
import io.oss.data.highway.model.{CsvToParquet, XlsxToCsv}
import io.oss.data.highway.utils.Constants.{XLS_EXTENSION, XLSX_EXTENSION, SEPARATOR}
import io.oss.data.highway.utils.{ParquetHandler, XlsxCsvConverter}
import org.apache.spark.sql.SaveMode

object App {

  def main(args: Array[String]): Unit = {
    for {
      conf <- ConfLoader.loadConf()
      _ <- conf.route match {
        case XlsxToCsv(in, out) => XlsxCsvConverter.apply(in, out, Seq(XLS_EXTENSION, XLSX_EXTENSION))
        case CsvToParquet(in, out) =>
          for {
            _ <- ParquetHandler.saveCsvAsParquet(in, out, SEPARATOR, SaveMode.Overwrite)
            df <-  ParquetHandler.readParquet(out)
          } yield df.show(false)
      }
    } yield ()
  }
}
