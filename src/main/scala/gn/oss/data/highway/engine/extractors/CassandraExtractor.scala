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
  DHErrorResponse,
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
        handleRoutesWithExplicitSaveModes(input, output, consist)
      case None =>
        handleRoutesWithImplicitSaveModes(input, output, temporaryPath, tempoBasePath)
    }
  }

  /**
    * Handles routes that uses implicit save modes. It handles the following outputs: Elasticsearch and Kafka.
    *
    * @param input The Cassandra entity
    * @param output The output plug: Elasticsearch or Kafka
    * @param temporaryPath The temporary path that will contain the intermediate JSON dataset that will be transferred to the output
    * @param tempoBasePath The base path for the intermediate JSON dataset
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  private def handleRoutesWithImplicitSaveModes(
      input: Cassandra,
      output: Output,
      temporaryPath: String,
      tempoBasePath: String
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    output match {
      case elasticsearch @ Elasticsearch(_, _, _) =>
        DataFrameUtils
          .loadDataFrame(CassandraDB(input.keyspace, input.table), Constants.EMPTY)
          .traverse(df => DataFrameUtils.saveDataFrame(df, JSON, temporaryPath, Append))
          .flatten
        val result = ElasticSink
          .insertDocuments(File(JSON, temporaryPath), elasticsearch, tempoBasePath, Local)
        SharedUtils.constructIOResponse(input, elasticsearch, result, SUCCESS)
      case kafka @ Kafka(_, _) =>
        DataFrameUtils
          .loadDataFrame(CassandraDB(input.keyspace, input.table), Constants.EMPTY)
          .traverse(df => DataFrameUtils.saveDataFrame(df, JSON, temporaryPath, Append))
          .flatten
        val result = KafkaSink.handleKafkaChannel(File(JSON, temporaryPath), kafka, Some(Local))
        SharedUtils.constructIOResponse(input, kafka, result.leftMap(_.toThrowable), SUCCESS)
      case _ =>
        Left(
          DHErrorResponse(
            "MissingSaveMode",
            "Missing 'save-mode' field",
            ""
          )
        )
    }
  }

  /**
    * Handles routes that uses explicit save modes. It handles the following outputs: File, Postgres and Cassandra.
    *
    * @param input The Cassandra entity
    * @param output The output plug: File, Postgres or Cassandra
    * @param consistency A representation for the Spark Save Mode
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  private def handleRoutesWithExplicitSaveModes(
      input: Cassandra,
      output: Output,
      consistency: Consistency
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    output match {
      case file @ File(dataType, path) =>
        val result = DataFrameUtils
          .loadDataFrame(CassandraDB(input.keyspace, input.table), Constants.EMPTY)
          .traverse(df => DataFrameUtils.saveDataFrame(df, dataType, path, consistency.toSaveMode))
          .flatten
        SharedUtils.constructIOResponse(input, file, result, SUCCESS)
      case cassandra @ Cassandra(keyspace, table) =>
        val result = DataFrameUtils
          .loadDataFrame(CassandraDB(input.keyspace, input.table), Constants.EMPTY)
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
      case postgres @ Postgres(database, table) =>
        val result = DataFrameUtils
          .loadDataFrame(CassandraDB(input.keyspace, input.table), Constants.EMPTY)
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
      case _ =>
        Left(
          DHErrorResponse(
            "ShouldUseIntermediateSaveMode",
            "'save-mode' field should be not present",
            ""
          )
        )
    }
  }
}
