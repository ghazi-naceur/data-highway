package io.oss.data.highway

import io.oss.data.highway.configuration.ConfLoader
import io.oss.data.highway.model.{CsvToParquet, XlsxToCsv}
import io.oss.data.highway.utils.{ParquetHandler, XlsxCsvConverter}
import org.apache.spark.sql.SaveMode

object App {

  def main(args: Array[String]): Unit = {
    for {
      conf <- ConfLoader.loadConf()
      _ <- conf.route match {
//        case XlsxToCsv(in, out) => XlsxCsvConverter.apply(in, out)
        case CsvToParquet(in, out) =>
          for {
            _ <- ParquetHandler.saveCsvAsParquet(in, out, ";", SaveMode.Overwrite)
            df <-  ParquetHandler.readParquet(out)
          } yield df.show(false)
      }
    } yield ()
  }
}
