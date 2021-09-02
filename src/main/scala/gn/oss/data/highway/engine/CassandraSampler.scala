package gn.oss.data.highway.engine

import org.apache.spark.sql.SaveMode
import cats.implicits._
import gn.oss.data.highway.models.{
  Cassandra,
  CassandraDB,
  Elasticsearch,
  File,
  JSON,
  Kafka,
  Local,
  Output
}
import gn.oss.data.highway.utils.{Constants, DataFrameUtils, SharedUtils}

object CassandraSampler {

  /**
    * Extracts rows from Cassandra and save them into files
    *
    * @param input The input Cassandra entity
    * @param output The output entity
    * @param saveMode The output save mode
    * @return a Unit, otherwise Throwable
    */
  def extractRows(
      input: Cassandra,
      output: Output,
      saveMode: SaveMode
  ): Either[Throwable, Any] = {
    val (temporaryPath, tempoBasePath) =
      SharedUtils.setTempoFilePath("cassandra-sampler", Some(Local))
    output match {
      case File(dataType, path) =>
        DataFrameUtils
          .loadDataFrame(CassandraDB(input.keyspace, input.table), Constants.EMPTY)
          .traverse(df => DataFrameUtils.saveDataFrame(df, dataType, path, saveMode))
          .flatten
      case Cassandra(keyspace, table) =>
        DataFrameUtils
          .loadDataFrame(CassandraDB(input.keyspace, input.table), Constants.EMPTY)
          .traverse(df =>
            DataFrameUtils
              .saveDataFrame(df, CassandraDB(keyspace, table), Constants.EMPTY, saveMode)
          )
          .flatten
      case elasticsearch @ Elasticsearch(_, _, _) =>
        DataFrameUtils
          .loadDataFrame(CassandraDB(input.keyspace, input.table), Constants.EMPTY)
          .traverse(df => DataFrameUtils.saveDataFrame(df, JSON, temporaryPath, saveMode))
          .flatten
        ElasticSink.insertDocuments(File(JSON, temporaryPath), elasticsearch, tempoBasePath, Local)
      case kafka @ Kafka(_, _) =>
        DataFrameUtils
          .loadDataFrame(CassandraDB(input.keyspace, input.table), Constants.EMPTY)
          .traverse(df => DataFrameUtils.saveDataFrame(df, JSON, temporaryPath, saveMode))
          .flatten
        KafkaSink.handleKafkaChannel(File(JSON, temporaryPath), kafka, Some(Local))
    }
  }
}