package gn.oss.data.highway.engine

import org.apache.spark.sql.SaveMode
import cats.implicits._
import gn.oss.data.highway.models.{
  Cassandra,
  CassandraDB,
  DataHighwayErrorResponse,
  DataHighwayResponse,
  Elasticsearch,
  File,
  JSON,
  Kafka,
  Local,
  Output
}
import gn.oss.data.highway.utils.Constants.SUCCESS
import gn.oss.data.highway.utils.{Constants, DataFrameUtils, SharedUtils}

object CassandraSampler {

  /**
    * Extracts rows from Cassandra and save them into files
    *
    * @param input The input Cassandra entity
    * @param output The output entity
    * @param saveMode The output save mode
    * @return DataHighwayFileResponse, otherwise a DataHighwayErrorResponse
    */
  def extractRows(
      input: Cassandra,
      output: Output,
      saveMode: SaveMode
  ): Either[DataHighwayErrorResponse, DataHighwayResponse] = {
    val (temporaryPath, tempoBasePath) =
      SharedUtils.setTempoFilePath("cassandra-sampler", Some(Local))
    output match {
      case file @ File(dataType, path) =>
        val result = DataFrameUtils
          .loadDataFrame(CassandraDB(input.keyspace, input.table), Constants.EMPTY)
          .traverse(df => DataFrameUtils.saveDataFrame(df, dataType, path, saveMode))
          .flatten
        SharedUtils.constructIOResponse(input, file, result, SUCCESS)
      case cassandra @ Cassandra(keyspace, table) =>
        val result = DataFrameUtils
          .loadDataFrame(CassandraDB(input.keyspace, input.table), Constants.EMPTY)
          .traverse(df =>
            DataFrameUtils
              .saveDataFrame(df, CassandraDB(keyspace, table), Constants.EMPTY, saveMode)
          )
          .flatten
        SharedUtils.constructIOResponse(input, cassandra, result, SUCCESS)
      case elasticsearch @ Elasticsearch(_, _, _) =>
        DataFrameUtils
          .loadDataFrame(CassandraDB(input.keyspace, input.table), Constants.EMPTY)
          .traverse(df => DataFrameUtils.saveDataFrame(df, JSON, temporaryPath, saveMode))
          .flatten
        val result = ElasticSink
          .insertDocuments(File(JSON, temporaryPath), elasticsearch, tempoBasePath, Local)
        SharedUtils
          .constructIOResponse(input, elasticsearch, result, SUCCESS)
      case kafka @ Kafka(_, _) =>
        DataFrameUtils
          .loadDataFrame(CassandraDB(input.keyspace, input.table), Constants.EMPTY)
          .traverse(df => DataFrameUtils.saveDataFrame(df, JSON, temporaryPath, saveMode))
          .flatten
        val result = KafkaSink.handleKafkaChannel(File(JSON, temporaryPath), kafka, Some(Local))
        SharedUtils
          .constructIOResponse(
            input,
            kafka,
            result.leftMap(_.toThrowable),
            SUCCESS
          )
    }
  }
}
