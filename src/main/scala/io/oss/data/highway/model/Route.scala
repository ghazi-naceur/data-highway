package io.oss.data.highway.model

import pureconfig.generic.FieldCoproductHint
import pureconfig.generic.ProductHint

sealed trait Channel

case object XlsxCsv extends Channel

case object CsvParquet extends Channel

case object ParquetCsv extends Channel

case object ParquetJson extends Channel

case object CsvJson extends Channel

sealed trait Route {
  val value: Channel
}

case class XlsxToCsv(in: String, out: String) extends Route {
  override val value: Channel = XlsxCsv
}

case class CsvToParquet(in: String, out: String) extends Route {
  override val value: Channel = CsvParquet
}

case class ParquetToCsv(in: String, out: String) extends Route {
  override val value: Channel = ParquetCsv
}

case class ParquetToJson(in: String, out: String) extends Route {
  override val value: Channel = ParquetJson
}

case class CsvToJson(in: String, out: String) extends Route {
  override val value: Channel = CsvJson
}
