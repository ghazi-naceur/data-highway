package io.oss.data.highway.model

sealed trait Route {
  val channel: Channel
}

case class XlsxToCsv(in: String, out: String) extends Route {
  override val channel: Channel = XlsxCsv
}

case class CsvToParquet(in: String, out: String) extends Route {
  override val channel: Channel = CsvParquet
}

case class JsonToParquet(in: String, out: String) extends Route {
  override val channel: Channel = JsonParquet
}

case class ParquetToCsv(in: String, out: String) extends Route {
  override val channel: Channel = ParquetCsv
}

case class AvroToCsv(in: String, out: String) extends Route {
  override val channel: Channel = AvroCsv
}

case class JsonToCsv(in: String, out: String) extends Route {
  override val channel: Channel = JsonCsv
}

case class ParquetToJson(in: String, out: String) extends Route {
  override val channel: Channel = ParquetJson
}

case class CsvToJson(in: String, out: String) extends Route {
  override val channel: Channel = CsvJson
}
case class JsonToKafka(in: String,
                       out: String,
                       brokerUrls: String,
                       kafkaMode: KafkaMode)
    extends Route {
  override val channel: Channel = JsonKafka
}

case class ParquetToAvro(in: String, out: String) extends Route {
  override val channel: Channel = ParquetAvro
}

case class JsonToAvro(in: String, out: String) extends Route {
  override val channel: Channel = JsonAvro
}

case class CsvToAvro(in: String, out: String) extends Route {
  override val channel: Channel = CsvAvro
}
