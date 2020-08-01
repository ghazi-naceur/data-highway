package io.oss.data.highway.model

sealed trait Route {
  val value: Channel
}

case class XlsxToCsv(in: String, out: String) extends Route {
  override val value: Channel = XlsxCsv
}

case class CsvToParquet(in: String, out: String) extends Route {
  override val value: Channel = CsvParquet
}

case class JsonToParquet(in: String, out: String) extends Route {
  override val value: Channel = JsonParquet
}

case class ParquetToCsv(in: String, out: String) extends Route {
  override val value: Channel = ParquetCsv
}

case class JsonToCsv(in: String, out: String) extends Route {
  override val value: Channel = JsonCsv
}

case class ParquetToJson(in: String, out: String) extends Route {
  override val value: Channel = ParquetJson
}

case class CsvToJson(in: String, out: String) extends Route {
  override val value: Channel = CsvJson
}
