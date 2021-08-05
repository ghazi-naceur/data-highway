package io.oss.data.highway.models

sealed trait Route

case class XlsxToCsv(in: String, out: String, storage: Storage) extends Route

case class CsvToParquet(in: String, out: String, storage: Storage) extends Route

case class JsonToParquet(in: String, out: String, storage: Storage) extends Route

case class AvroToParquet(in: String, out: String, storage: Storage) extends Route

case class ParquetToCsv(in: String, out: String, storage: Storage) extends Route

case class AvroToCsv(in: String, out: String, storage: Storage) extends Route

case class JsonToCsv(in: String, out: String, storage: Storage) extends Route

case class ParquetToJson(in: String, out: String, storage: Storage) extends Route

case class AvroToJson(in: String, out: String, storage: Storage) extends Route

case class CsvToJson(in: String, out: String, storage: Storage) extends Route

case class FileToKafka(in: String, out: String, storage: Storage, kafkaMode: KafkaMode)
    extends Route

case class ParquetToAvro(in: String, out: String, storage: Storage) extends Route

case class JsonToAvro(in: String, out: String, storage: Storage) extends Route

case class CsvToAvro(in: String, out: String, storage: Storage) extends Route

case class KafkaToFile(in: String, out: String, storage: Storage, kafkaMode: KafkaMode)
    extends Route

case class KafkaToKafka(in: String, out: String, kafkaMode: KafkaMode) extends Route

case class FileToElasticsearch(
    in: String,
    out: String,
    storage: Storage,
    bulkEnabled: Boolean
) extends Route

case class ElasticsearchToFile(
    in: String,
    out: String,
    storage: Storage,
    searchQuery: SearchQuery
) extends Route

case class ElasticOps(operation: ElasticOperation) extends Route

case class FileToCassandra(in: String, cassandra: Cassandra, storage: Storage, dataType: DataType)
    extends Route
