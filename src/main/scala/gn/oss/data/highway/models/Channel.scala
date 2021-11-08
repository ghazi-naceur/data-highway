package gn.oss.data.highway.models

sealed trait Channel

sealed trait Plug
object Plug {

  /**
    * Gives a summary for the provided Plug, by extracting the main properties and return them as a String.
    *
    * @param plug The provided plug. It can be an input or an output.
    * @return String
    */
  def summary(plug: Plug): String = {
    plug match {
      case File(dataType, path)       => s"File: Path '$path' in '$dataType' format"
      case Cassandra(keyspace, table) => s"Cassandra: Keyspace '$keyspace' - Table '$table'"
      case Postgres(database, table)  => s"Postgres: Database '$database' - Table '$table'"
      case Elasticsearch(index, _, _) => s"Elasticsearch: Index '$index'"
      case Kafka(topic, optKafkaMode) =>
        optKafkaMode match {
          case Some(kafkaMode) =>
            kafkaMode match {
              case PureKafkaStreamsConsumer(_, _, _) | PureKafkaConsumer(_, _, _) =>
                s"Trigger Streaming job - Kafka: Topic '$topic'"
              case _ => s"Kafka: Topic '$topic'"
            }
          case scala.None => s"Kafka: Topic '$topic'"
        }
    }
  }
}

sealed trait Input extends Plug
sealed trait Output extends Plug

case class Route(input: Input, output: Output, storage: Option[Storage], saveMode: Option[Consistency]) extends Channel

case class File(dataType: DataType, path: String) extends Input with Output
case class Cassandra(keyspace: String, table: String) extends Input with Output
case class Postgres(database: String, table: String) extends Input with Output
case class Elasticsearch(index: String, bulkEnabled: Boolean, searchQuery: Option[SearchQuery]) extends Input with Output
case class Kafka(topic: String, kafkaMode: Option[KafkaMode]) extends Input with Output

sealed trait Query extends Channel

case class ElasticOps(operation: ElasticOperation) extends Query
