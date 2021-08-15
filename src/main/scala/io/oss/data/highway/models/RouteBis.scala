package io.oss.data.highway.models

sealed trait RouteBis

case class FileToKafka(in: String, out: String, storage: Storage, kafkaMode: KafkaMode)
    extends RouteBis

case class ParquetToAvro(in: String, out: String, storage: Storage) extends RouteBis

case class JsonToAvro(in: String, out: String, storage: Storage) extends RouteBis

case class CsvToAvro(in: String, out: String, storage: Storage) extends RouteBis

case class KafkaToFile(in: String, out: String, storage: Storage, kafkaMode: KafkaMode)
    extends RouteBis

case class KafkaToKafka(in: String, out: String, kafkaMode: KafkaMode) extends RouteBis

case class FileToElasticsearch(
    in: String,
    out: String,
    storage: Storage,
    bulkEnabled: Boolean
) extends RouteBis

case class ElasticsearchToFile(
    in: String,
    out: String,
    storage: Storage,
    searchQuery: SearchQuery
) extends RouteBis

case class ElasticOps(operation: ElasticOperation) extends RouteBis

case class FileToCassandra(in: String, cassandra: CassandraDB, storage: Storage, dataType: DataType)
    extends RouteBis

case class CassandraToFile(cassandra: CassandraDB, out: String, dataType: DataType) extends RouteBis

sealed trait Input
sealed trait Output
case class Route(input: Input, output: Output, storage: Option[Storage]) extends RouteBis
case class File(dataType: DataType, path: String)                        extends Input with Output
case class Cassandra(keyspace: String, table: String)                    extends Input with Output
case class Elasticsearch(index: String, bulkEnabled: Boolean)            extends Input with Output
case class Kafka(topic: String, kafkaMode: KafkaMode)                    extends Input with Output
