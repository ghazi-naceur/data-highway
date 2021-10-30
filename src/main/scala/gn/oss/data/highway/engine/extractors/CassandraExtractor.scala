package gn.oss.data.highway.engine.extractors

import gn.oss.data.highway.engine.sinks.{ElasticSink, KafkaSink}
import gn.oss.data.highway.utils.Constants.EMPTY
import gn.oss.data.highway.utils.{DataFrameUtils, SharedUtils}
import org.apache.spark.sql.SaveMode.Append
import cats.implicits._
import gn.oss.data.highway.models.DataHighwayRuntimeException.{
  MustHaveExplicitSaveModeError,
  MustNotHaveExplicitSaveModeError
}
import gn.oss.data.highway.models.{
  Cassandra,
  CassandraDB,
  Consistency,
  DataHighwayErrorResponse,
  DataHighwaySuccessResponse,
  Elasticsearch,
  File,
  JSON,
  Kafka,
  Local,
  Output,
  Postgres,
  PostgresDB
}

object CassandraExtractor {

  /**
    * Extracts rows from Cassandra and save them into files
    *
    * @param input The input Cassandra entity
    * @param output The output entity
    * @param consistency The output save mode
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  def extractRows(
      input: Cassandra,
      output: Output,
      consistency: Option[Consistency]
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    val (temporaryPath, tempoBasePath) =
      SharedUtils.setTempoFilePath("cassandra-extractor", Some(Local))
    consistency match {
      case Some(consist) =>
        handleRouteWithExplicitSaveMode(input, output, consist)
      case None =>
        handleRouteWithImplicitSaveMode(input, output, temporaryPath, tempoBasePath)
    }
  }

  /**
    * Handles route that uses implicit save modes. It handles the following outputs: Elasticsearch and Kafka.
    *
    * @param input The Cassandra entity
    * @param output The output plug: Elasticsearch or Kafka
    * @param temporaryPath The temporary path that will contain the intermediate JSON dataset that will be transferred to the output
    * @param tempoBasePath The base path for the intermediate JSON dataset
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  private def handleRouteWithImplicitSaveMode(
      input: Cassandra,
      output: Output,
      temporaryPath: String,
      tempoBasePath: String
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    // todo loadDataFrame x2
    val result = output match {
      case elasticsearch @ Elasticsearch(_, _, _) =>
        DataFrameUtils
          .loadDataFrame(CassandraDB(input.keyspace, input.table), EMPTY)
          .traverse(df => DataFrameUtils.saveDataFrame(df, JSON, temporaryPath, Append))
          .flatten
        ElasticSink.insertDocuments(File(JSON, temporaryPath), elasticsearch, tempoBasePath, Local)
      case kafka @ Kafka(_, _) =>
        DataFrameUtils
          .loadDataFrame(CassandraDB(input.keyspace, input.table), EMPTY)
          .traverse(df => DataFrameUtils.saveDataFrame(df, JSON, temporaryPath, Append))
          .flatten
        KafkaSink.handleKafkaChannel(File(JSON, temporaryPath), kafka, Some(Local))
      case _ => Left(MustNotHaveExplicitSaveModeError)
    }
    SharedUtils.constructIOResponse(input, output, result)
  }

  /**
    * Handles route that uses explicit save modes. It handles the following outputs: File, Postgres and Cassandra.
    *
    * @param input The Cassandra entity
    * @param output The output plug: File, Postgres or Cassandra
    * @param consistency A representation for the Spark Save Mode
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  private def handleRouteWithExplicitSaveMode(
      input: Cassandra,
      output: Output,
      consistency: Consistency
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    // todo loadDataFrame x3
    val result = output match {
      case File(dataType, path) =>
        DataFrameUtils
          .loadDataFrame(CassandraDB(input.keyspace, input.table), EMPTY)
          .traverse(df => DataFrameUtils.saveDataFrame(df, dataType, path, consistency.toSaveMode))
          .flatten
      case Cassandra(keyspace, table) =>
        DataFrameUtils
          .loadDataFrame(CassandraDB(input.keyspace, input.table), EMPTY)
          .traverse(df =>
            DataFrameUtils
              .saveDataFrame(df, CassandraDB(keyspace, table), EMPTY, consistency.toSaveMode)
          )
          .flatten
      case Postgres(database, table) =>
        DataFrameUtils
          .loadDataFrame(CassandraDB(input.keyspace, input.table), EMPTY)
          .traverse(df =>
            DataFrameUtils
              .saveDataFrame(df, PostgresDB(database, table), EMPTY, consistency.toSaveMode)
          )
          .flatten
      case _ => Left(MustHaveExplicitSaveModeError)
    }
    SharedUtils.constructIOResponse(input, output, result)
  }
}
