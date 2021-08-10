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

case class RouteBis(input: Input, output: Output)
case class File(dataType: DataType, path: String)        extends Input with Output
case class CassandraBis(keyspace: String, table: String) extends Input with Output
case class Elasticsearch(index: String)                  extends Input with Output
case class Kafka(topic: String, kafkaMode: KafkaMode)    extends Input with Output

// todo to be removed
object Main {
  def main(args: Array[String]): Unit = {
    println(loadRouteBisConf())
//    RouteBis(File(CSV, ""), CassandraBis("", "")) match {
//      case RouteBis(input: File, output: File)          =>
//      case RouteBis(input: File, output: CassandraBis)  =>
//      case RouteBis(input: CassandraBis, output: File)  =>
//      case RouteBis(input: File, output: Kafka)         => // todo only json is supported, make other types supported too
//      case RouteBis(input: Kafka, output: File)         => // todo only json is supported, make other types supported too
//      case RouteBis(input: File, output: Elasticsearch) => // todo only json is supported, make other types supported too
//      case RouteBis(input: Elasticsearch, output: File) => // todo only json is supported, make other types supported too
//    }

    def loadRouteBisConf(): RouteBis = {
      import pureconfig._
      import pureconfig.generic.auto._ // To be kept, even though intellij didn't recognize its usage

      implicit val offsetConvert: ConfigReader[LogLevel] =
        deriveEnumerationReader[LogLevel]

      ConfigSource.default
        .at("route-bis")
        .load[RouteBis] match {
        case Right(conf) => conf
        case Left(thr) =>
          throw new RuntimeException(
            s"Error when trying to load Spark configuration : ${thr.toList.mkString("\n")}"
          )
      }
    }
  }
}
