package gn.oss.data.highway.models

sealed trait Channel

sealed trait Input
sealed trait Output

case class Route(input: Input, output: Output, storage: Option[Storage]) extends Channel

case class File(dataType: DataType, path: String)     extends Input with Output
case class Cassandra(keyspace: String, table: String) extends Input with Output
case class Elasticsearch(index: String, bulkEnabled: Boolean, searchQuery: Option[SearchQuery])
    extends Input
    with Output
case class Kafka(topic: String, kafkaMode: Option[KafkaMode]) extends Input with Output

sealed trait Query extends Channel

case class ElasticOps(operation: ElasticOperation) extends Query
