package gn.oss.data.highway.engine.extractors

import gn.oss.data.highway.engine.sinks.{ElasticSink, KafkaSink}
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
import gn.oss.data.highway.utils.Constants.EMPTY

object PostgresExtractor {

  /**
    * Extracts rows from Postgres and save them into files
    *
    * @param input The input Postgres entity
    * @param output The output entity
    * @param consistency The output save mode
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  def extractRows(
      input: Postgres,
      output: Output,
      consistency: Option[Consistency]
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    val (temporaryPath, tempoBasePath) =
      SharedUtils.setTempoFilePath("postgres-extractor", Some(Local))
    consistency match {
      case Some(consist) =>
        handleRouteWithExplicitSaveMode(input, output, consist)
      case None =>
        handleRouteWithImplicitSaveMode(input, output, temporaryPath, tempoBasePath)
    }
  }

  private def handleRouteWithImplicitSaveMode(
      input: Postgres,
      output: Output,
      temporaryPath: String,
      tempoBasePath: String
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    val result = output match {
      case elasticsearch @ Elasticsearch(_, _, _) =>
        DataFrameUtils
          .loadDataFrame(PostgresDB(input.database, input.table), EMPTY)
          .traverse(df => DataFrameUtils.saveDataFrame(df, JSON, temporaryPath, Append))
          .flatten
        ElasticSink.insertDocuments(File(JSON, temporaryPath), elasticsearch, tempoBasePath, Local)
      case kafka @ Kafka(_, _) =>
        DataFrameUtils
          .loadDataFrame(PostgresDB(input.database, input.table), EMPTY)
          .traverse(df => DataFrameUtils.saveDataFrame(df, JSON, temporaryPath, Append))
          .flatten
        KafkaSink.handleKafkaChannel(File(JSON, temporaryPath), kafka, Some(Local))
      case _ => Left(MustNotHaveExplicitSaveModeError)
    }
    SharedUtils.constructIOResponse(input, output, result)
  }

  private def handleRouteWithExplicitSaveMode(
      input: Postgres,
      output: Output,
      consistency: Consistency
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    val result = output match {
      case File(dataType, path) =>
        DataFrameUtils
          .loadDataFrame(PostgresDB(input.database, input.table), EMPTY)
          .traverse(df => DataFrameUtils.saveDataFrame(df, dataType, path, consistency.toSaveMode))
          .flatten
      case Postgres(database, table) =>
        DataFrameUtils
          .loadDataFrame(PostgresDB(input.database, input.table), EMPTY)
          .traverse(df =>
            DataFrameUtils
              .saveDataFrame(df, PostgresDB(database, table), EMPTY, consistency.toSaveMode)
          )
          .flatten
      case Cassandra(keyspace, table) =>
        DataFrameUtils
          .loadDataFrame(PostgresDB(input.database, input.table), EMPTY)
          .traverse(df =>
            DataFrameUtils
              .saveDataFrame(df, CassandraDB(keyspace, table), EMPTY, consistency.toSaveMode)
          )
          .flatten
      case _ => Left(MustHaveExplicitSaveModeError)
    }
    SharedUtils.constructIOResponse(input, output, result)
  }
}
