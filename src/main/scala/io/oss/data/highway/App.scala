package io.oss.data.highway

import io.oss.data.highway.configuration.ConfLoader
import io.oss.data.highway.model.XlsxToCsv
import io.oss.data.highway.utils.XlsxCsvConverter

object App {

  def main(args: Array[String]): Unit = {
    for {
      conf <- ConfLoader.loadConf()
      _ <- conf.route match {
        case XlsxToCsv(in, out) => XlsxCsvConverter.apply(in, out)
      }
    } yield ()
  }
}
