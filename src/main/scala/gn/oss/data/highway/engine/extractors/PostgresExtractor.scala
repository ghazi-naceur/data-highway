package gn.oss.data.highway.engine.extractors

import gn.oss.data.highway.engine.sinks.{ElasticSink, KafkaSink}
import gn.oss.data.highway.utils.Constants.SUCCESS
import gn.oss.data.highway.utils.{Constants, DataFrameUtils, SharedUtils}
import org.apache.spark.sql.SaveMode.Append
import cats.implicits._
import gn.oss.data.highway.models.{
  Cassandra,
  CassandraDB,
  Consistency,
  DataHighwayErrorResponse,
  DataHighwayResponse,
  Elasticsearch,
  File,
  JSON,
  Kafka,
  Local,
  Output,
  Postgres,
  PostgresDB
}

object PostgresExtractor {

  /**
    * Extracts rows from Postgres and save them into files
    *
    * @param input The input Postgres entity
    * @param output The output entity
    * @param consistency The output save mode
    * @return DataHighwayFileResponse, otherwise a DataHighwayErrorResponse
    */
  def extractRows(
      input: Postgres,
      output: Output,
      consistency: Option[Consistency]
  ): Either[DataHighwayErrorResponse, DataHighwayResponse] = {
    val (temporaryPath, tempoBasePath) =
      SharedUtils.setTempoFilePath("postgres-sampler", Some(Local))
    consistency match {
      case Some(consist) =>
        handleRoutesWithExplicitSaveModes(input, output, consist)
      case None =>
        handleRoutesWithIntermediateSaveModes(input, output, temporaryPath, tempoBasePath)
    }
  }

  private def handleRoutesWithIntermediateSaveModes(
      input: Postgres,
      output: Output,
      temporaryPath: String,
      tempoBasePath: String
  ): Either[DataHighwayErrorResponse, DataHighwayResponse] = {
    output match {
      case elasticsearch @ Elasticsearch(_, _, _) =>
        DataFrameUtils
          .loadDataFrame(PostgresDB(input.database, input.table), Constants.EMPTY)
          .traverse(df => DataFrameUtils.saveDataFrame(df, JSON, temporaryPath, Append))
          .flatten
        val result = ElasticSink
          .insertDocuments(File(JSON, temporaryPath), elasticsearch, tempoBasePath, Local)
        SharedUtils
          .constructIOResponse(input, elasticsearch, result, SUCCESS)
      case kafka @ Kafka(_, _) =>
        DataFrameUtils
          .loadDataFrame(PostgresDB(input.database, input.table), Constants.EMPTY)
          .traverse(df => DataFrameUtils.saveDataFrame(df, JSON, temporaryPath, Append))
          .flatten
        val result = KafkaSink.handleKafkaChannel(File(JSON, temporaryPath), kafka, Some(Local))
        SharedUtils
          .constructIOResponse(
            input,
            kafka,
            result.leftMap(_.toThrowable),
            SUCCESS
          )
      case _ =>
        Left(
          DataHighwayErrorResponse(
            "MissingSaveMode",
            "Missing 'save-mode' field",
            ""
          )
        )
    }
  }

  private def handleRoutesWithExplicitSaveModes(
      input: Postgres,
      output: Output,
      consistency: Consistency
  ): Either[DataHighwayErrorResponse, DataHighwayResponse] = {
    output match {
      case file @ File(dataType, path) =>
        val result = DataFrameUtils
          .loadDataFrame(PostgresDB(input.database, input.table), Constants.EMPTY)
          .traverse(df => DataFrameUtils.saveDataFrame(df, dataType, path, consistency.toSaveMode))
          .flatten
        SharedUtils.constructIOResponse(input, file, result, SUCCESS)
      case postgres @ Postgres(database, table) =>
        val result = DataFrameUtils
          .loadDataFrame(PostgresDB(input.database, input.table), Constants.EMPTY)
          .traverse(df =>
            DataFrameUtils
              .saveDataFrame(
                df,
                PostgresDB(database, table),
                Constants.EMPTY,
                consistency.toSaveMode
              )
          )
          .flatten
        SharedUtils.constructIOResponse(input, postgres, result, SUCCESS)
      case cassandra @ Cassandra(keyspace, table) =>
        val result = DataFrameUtils
          .loadDataFrame(PostgresDB(input.database, input.table), Constants.EMPTY)
          .traverse(df =>
            DataFrameUtils
              .saveDataFrame(
                df,
                CassandraDB(keyspace, table),
                Constants.EMPTY,
                consistency.toSaveMode
              )
          )
          .flatten
        SharedUtils.constructIOResponse(input, cassandra, result, SUCCESS)
      case _ =>
        Left(
          DataHighwayErrorResponse(
            "ShouldUseIntermediateSaveMode",
            "'save-mode' field should be not present",
            ""
          )
        )
    }
  }
}
