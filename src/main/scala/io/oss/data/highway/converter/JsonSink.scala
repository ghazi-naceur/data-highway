package io.oss.data.highway.converter

import io.oss.data.highway.model.DataHighwayError.JsonError
import io.oss.data.highway.model.{
  AVRO,
  AvroJson,
  CSV,
  Channel,
  CsvJson,
  DataHighwayError,
  JSON,
  PARQUET,
  ParquetJson
}
import io.oss.data.highway.utils.{DataFrameUtils, FilesUtils}
import org.apache.spark.sql.{DataFrame, SaveMode}
import cats.implicits._
import io.oss.data.highway.configuration.SparkConfig

object JsonSink {

  /**
    * Save parquet file as json
    *
    * @param in       The input parquet path
    * @param out      The generated json file path
    * @param saveMode The file saving mode
    * @param sparkConfig The Spark Configuration
    * @return Unit if successful, otherwise Error
    */
  def saveParquetAsJson(in: String,
                        out: String,
                        saveMode: SaveMode,
                        sparkConfig: SparkConfig): Either[JsonError, Unit] = {
    DataFrameUtils(sparkConfig)
      .loadDataFrame(in, PARQUET)
      .map(df => {
        df.coalesce(1)
          .write
          .mode(saveMode)
          .json(out)
      })
      .leftMap(thr =>
        JsonError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Save csv file as json
    *
    * @param in       The input csv path
    * @param out      The generated json file path
    * @param saveMode The file saving mode
    * @param sparkConfig The Spark Configuration
    * @return Unit if successful, otherwise Error
    */
  def saveCsvAsJson(in: String,
                    out: String,
                    saveMode: SaveMode,
                    sparkConfig: SparkConfig): Either[JsonError, Unit] = {
    DataFrameUtils(sparkConfig)
      .loadDataFrame(in, CSV)
      .map(df => {
        df.coalesce(1)
          .write
          .mode(saveMode)
          .json(out)
      })
      .leftMap(thr =>
        JsonError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Save avro file as json
    *
    * @param in       The input avro path
    * @param out      The generated json file path
    * @param saveMode The file saving mode
    * @param sparkConfig The Spark Configuration
    * @return Unit if successful, otherwise Error
    */
  def saveAvroAsJson(in: String,
                     out: String,
                     saveMode: SaveMode,
                     sparkConfig: SparkConfig): Either[JsonError, Unit] = {
    DataFrameUtils(sparkConfig)
      .loadDataFrame(in, AVRO)
      .map(df => {
        df.coalesce(1)
          .write
          .mode(saveMode)
          .json(out)
      })
      .leftMap(thr =>
        JsonError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Reads json file
    *
    * @param path The json file path
    * @param sparkConfig The Spark Configuration
    * @return DataFrame, otherwise Error
    */
  def readJson(path: String,
               sparkConfig: SparkConfig): Either[JsonError, DataFrame] = {
    DataFrameUtils(sparkConfig)
      .loadDataFrame(path, JSON)
      .leftMap(thr =>
        JsonError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  def apply(in: String,
            out: String,
            channel: Channel,
            saveMode: SaveMode,
            sparkConfig: SparkConfig): Either[DataHighwayError, List[Unit]] = {
    channel match {
      case ParquetJson =>
        handleParquetJsonChannel(in, out, saveMode, sparkConfig)
      case CsvJson =>
        handleCsvJsonChannel(in, out, saveMode, sparkConfig)
      case AvroJson =>
        handleAvroJsonChannel(in, out, saveMode, sparkConfig)
      case _ =>
        throw new RuntimeException("Not suppose to happen !")
    }
  }

  /**
    * Converts csv files to json files
    *
    * @param in        The input csv path
    * @param out       The generated json file path
    * @param saveMode  The file saving mode
    * @param sparkConfig The Spark Configuration
    * @return List[Unit], otherwise Error
    */
  private def handleCsvJsonChannel(
      in: String,
      out: String,
      saveMode: SaveMode,
      sparkConfig: SparkConfig): Either[DataHighwayError, List[Unit]] = {
    for {
      folders <- FilesUtils.listFoldersRecursively(in)
      list <- folders
        .traverse(folder => {
          val suffix =
            FilesUtils.reversePathSeparator(folder).split("/").last
          saveCsvAsJson(folder, s"$out/$suffix", saveMode, sparkConfig)
        })
        .leftMap(error =>
          JsonError(error.message, error.cause, error.stacktrace))
    } yield list
  }

  /**
    * Converts avro files to json files
    *
    * @param in        The input avro path
    * @param out       The generated json file path
    * @param saveMode  The file saving mode
    * @param sparkConfig The Spark Configuration
    * @return List[Unit], otherwise Error
    */
  private def handleAvroJsonChannel(
      in: String,
      out: String,
      saveMode: SaveMode,
      sparkConfig: SparkConfig): Either[DataHighwayError, List[Unit]] = {
    for {
      folders <- FilesUtils.listFoldersRecursively(in)
      list <- folders
        .traverse(folder => {
          val suffix =
            FilesUtils.reversePathSeparator(folder).split("/").last
          saveAvroAsJson(folder, s"$out/$suffix", saveMode, sparkConfig)
        })
        .leftMap(error =>
          JsonError(error.message, error.cause, error.stacktrace))
    } yield list
  }

  /**
    * Converts parquet files to json files
    *
    * @param in       The input parquet path
    * @param out      The generated json file path
    * @param saveMode The file saving mode
    * @param sparkConfig The Spark Configuration
    * @return List[Unit], otherwise Error
    */
  private def handleParquetJsonChannel(
      in: String,
      out: String,
      saveMode: SaveMode,
      sparkConfig: SparkConfig): Either[DataHighwayError, List[Unit]] = {
    for {
      folders <- FilesUtils.listFoldersRecursively(in)
      list <- folders
        .traverse(folder => {
          val suffix =
            FilesUtils.reversePathSeparator(folder).split("/").last
          saveParquetAsJson(folder, s"$out/$suffix", saveMode, sparkConfig)
        })
        .leftMap(error =>
          JsonError(error.message, error.cause, error.stacktrace))
    } yield list
  }
}
