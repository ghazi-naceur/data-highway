package io.oss.data.highway.models

import pureconfig.generic.semiauto.deriveEnumerationReader

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

case class CassandraToFile(cassandra: Cassandra, out: String, dataType: DataType) extends Route

sealed trait Input
sealed trait Output
// todo to be renamed Route instead of RouteBis
case class RouteBis(input: Input, output: Output, storage: Option[Storage]) extends Route
case class File(dataType: DataType, path: String)                           extends Input with Output
// todo to be renamed Route instead of CassandraBis
case class CassandraBis(keyspace: String, table: String) extends Input with Output
case class Elasticsearch(index: String)                  extends Input with Output
case class Kafka(topic: String, kafkaMode: KafkaMode)    extends Input with Output
