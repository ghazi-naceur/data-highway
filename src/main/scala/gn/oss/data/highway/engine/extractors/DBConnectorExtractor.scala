package gn.oss.data.highway.engine.extractors

import gn.oss.data.highway.engine.sinks.{ElasticSink, KafkaSink}
import gn.oss.data.highway.utils.Constants.EMPTY
import gn.oss.data.highway.utils.{DataFrameUtils, SharedUtils}
import org.apache.spark.sql.SaveMode.Append
import cats.implicits._
import gn.oss.data.highway.models.DataHighwayRuntimeException.{MustHaveSaveModeError, MustNotHaveSaveModeError}
import gn.oss.data.highway.models.{
  Cassandra,
  CassandraDB,
  Consistency,
  DBConnector,
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

object DBConnectorExtractor {

  /**
    * Extracts rows from Cassandra and save them into files
    *
    * @param input The input DBConnector entity: Cassandra or Postgres
    * @param output The output entity
    * @param consistency The output save mode
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  def extractRows(
    input: DBConnector,
    output: Output,
    consistency: Option[Consistency]
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    consistency match {
      case Some(consist) => handleRouteWithExplicitSaveMode(input, output, consist)
      case None          => handleRouteWithImplicitSaveMode(input, output)
    }
  }

  /**
    * Handles route that uses implicit save modes. It handles the following outputs: Elasticsearch and Kafka.
    *
    * @param input The Cassandra entity
    * @param output The output plug: Elasticsearch or Kafka
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  private def handleRouteWithImplicitSaveMode(
    input: DBConnector,
    output: Output
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    val result = input match {
      case Cassandra(keyspace, table) =>
        val temporaryLocation = SharedUtils.setTempoFilePath("cassandra-extractor", Some(Local))
        output match {
          case elasticsearch @ Elasticsearch(_, _, _) =>
            loadCassandraTableAsJsonFile(keyspace, table, temporaryLocation.path)
            ElasticSink
              .insertDocuments(File(JSON, temporaryLocation.path), elasticsearch, temporaryLocation.basePath, Local)
          case kafka @ Kafka(_, _) =>
            loadCassandraTableAsJsonFile(keyspace, table, temporaryLocation.path)
            KafkaSink.handleKafkaChannel(File(JSON, temporaryLocation.path), kafka, Some(Local))
          case _ => Left(MustNotHaveSaveModeError)
        }
      case Postgres(database, table) =>
        val temporaryLocation = SharedUtils.setTempoFilePath("postgres-extractor", Some(Local))
        output match {
          case elasticsearch @ Elasticsearch(_, _, _) =>
            loadPostgresTableAsJsonFile(database, table, temporaryLocation.path)
            ElasticSink
              .insertDocuments(File(JSON, temporaryLocation.path), elasticsearch, temporaryLocation.basePath, Local)
          case kafka @ Kafka(_, _) =>
            loadPostgresTableAsJsonFile(database, table, temporaryLocation.path)
            KafkaSink.handleKafkaChannel(File(JSON, temporaryLocation.path), kafka, Some(Local))
          case _ => Left(MustNotHaveSaveModeError)
        }
    }
    SharedUtils.constructIOResponse(input, output, result)
  }

  /**
    * Loads a Cassandra Table as a JSON file
    *
    * @param keyspace The Cassandra keyspace
    * @param table The Cassandra table
    * @param path The json file path
    * @return Unit, otherwise a Throwable
    */
  private def loadCassandraTableAsJsonFile(keyspace: String, table: String, path: String): Either[Throwable, Unit] = {
    DataFrameUtils
      .loadDataFrame(CassandraDB(keyspace, table), EMPTY)
      .traverse(df => DataFrameUtils.saveDataFrame(df, JSON, path, Append))
      .flatten
  }

  /**
    * Loads a Postgres Table as a JSON file
    *
    * @param database The Postgres keyspace
    * @param table The Postgres table
    * @param path The json file path
    * @return Unit, otherwise a Throwable
    */
  private def loadPostgresTableAsJsonFile(database: String, table: String, path: String): Either[Throwable, Unit] = {
    DataFrameUtils
      .loadDataFrame(PostgresDB(database, table), EMPTY)
      .traverse(df => DataFrameUtils.saveDataFrame(df, JSON, path, Append))
      .flatten
  }

  /**
    * Handles route that uses explicit save modes. It handles the following outputs: File, Postgres and Cassandra.
    *
    * @param input The input DBConnector entity: Cassandra or Postgres
    * @param output The output plug: File, Postgres or Cassandra
    * @param consistency A representation for the Spark Save Mode
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  private def handleRouteWithExplicitSaveMode(
    input: DBConnector,
    output: Output,
    consistency: Consistency
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    val result = input match {
      case Cassandra(keyspace, table) =>
        output match {
          case File(dataType, path) =>
            DataFrameUtils
              .loadDataFrame(CassandraDB(keyspace, table), EMPTY)
              .traverse(df => DataFrameUtils.saveDataFrame(df, dataType, path, consistency.toSaveMode))
              .flatten
          case Cassandra(keyspace, table) =>
            DataFrameUtils
              .loadDataFrame(CassandraDB(keyspace, table), EMPTY)
              .traverse(
                df => DataFrameUtils.saveDataFrame(df, CassandraDB(keyspace, table), EMPTY, consistency.toSaveMode)
              )
              .flatten
          case Postgres(database, table) =>
            DataFrameUtils
              .loadDataFrame(CassandraDB(keyspace, table), EMPTY)
              .traverse(
                df => DataFrameUtils.saveDataFrame(df, PostgresDB(database, table), EMPTY, consistency.toSaveMode)
              )
              .flatten
          case _ => Left(MustHaveSaveModeError)
        }
      case Postgres(database, table) =>
        output match {
          case File(dataType, path) =>
            DataFrameUtils
              .loadDataFrame(PostgresDB(database, table), EMPTY)
              .traverse(df => DataFrameUtils.saveDataFrame(df, dataType, path, consistency.toSaveMode))
              .flatten
          case Postgres(database, table) =>
            DataFrameUtils
              .loadDataFrame(PostgresDB(database, table), EMPTY)
              .traverse(
                df => DataFrameUtils.saveDataFrame(df, PostgresDB(database, table), EMPTY, consistency.toSaveMode)
              )
              .flatten
          case Cassandra(keyspace, table) =>
            DataFrameUtils
              .loadDataFrame(PostgresDB(database, table), EMPTY)
              .traverse(
                df => DataFrameUtils.saveDataFrame(df, CassandraDB(keyspace, table), EMPTY, consistency.toSaveMode)
              )
              .flatten
          case _ => Left(MustHaveSaveModeError)
        }
    }
    SharedUtils.constructIOResponse(input, output, result)
  }
}
