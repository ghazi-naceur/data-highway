package io.oss.data.highway.model

sealed trait Route

case class XlsxToCsv(in: String, out: String) extends Route

case class CsvToParquet(in: String, out: String) extends Route

case class JsonToParquet(in: String, out: String) extends Route

case class AvroToParquet(in: String, out: String) extends Route

case class ParquetToCsv(in: String, out: String) extends Route

case class AvroToCsv(in: String, out: String) extends Route

case class JsonToCsv(in: String, out: String) extends Route

case class ParquetToJson(in: String, out: String) extends Route

case class AvroToJson(in: String, out: String) extends Route

case class CsvToJson(in: String, out: String) extends Route

case class FileToKafka(in: String,
                       out: String,
                       brokerUrls: String,
                       kafkaMode: KafkaMode)
    extends Route

case class ParquetToAvro(in: String, out: String) extends Route

case class JsonToAvro(in: String, out: String) extends Route

case class CsvToAvro(in: String, out: String) extends Route

case class KafkaToFile(in: String,
                       out: String,
                       dataType: Option[DataType],
                       brokerUrls: String,
                       kafkaMode: KafkaMode,
                       offset: Offset,
                       consumerGroup: String)
    extends Route
