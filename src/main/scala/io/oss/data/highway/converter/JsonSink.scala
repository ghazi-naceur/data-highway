package io.oss.data.highway.converter

import io.oss.data.highway.model.DataHighwayError.JsonError
import io.oss.data.highway.model.{
  Channel,
  CsvJson,
  DataHighwayError,
  ParquetJson
}
import io.oss.data.highway.utils.FilesUtils
import org.apache.spark.sql.{DataFrame, SaveMode}
import cats.implicits._
import io.oss.data.highway.utils.DataFrameUtils.sparkSession

object JsonSink {

  /**
    * Save parquet file as json
    *
    * @param in       The input parquet path
    * @param out      The generated json file path
    * @param saveMode The file saving mode
    * @return Unit if successful, otherwise Error
    */
  def saveParquetAsJson(in: String,
                        out: String,
                        saveMode: SaveMode): Either[JsonError, Unit] = {
    Either
      .catchNonFatal {
        // TODO Use DataFrameUtils.loadDataFrame
        sparkSession.read
          .parquet(in)
          .coalesce(1)
          .write
          .mode(saveMode)
          .json(out)
      }
      .leftMap(thr =>
        JsonError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Save csv file as json
    *
    * @param in       The input csv path
    * @param out      The generated json file path
    * @param saveMode The file saving mode
    * @return Unit if successful, otherwise Error
    */
  def saveCsvAsJson(in: String,
                    out: String,
                    columnSeparator: String,
                    saveMode: SaveMode): Either[JsonError, Unit] = {
    Either
      .catchNonFatal {
        sparkSession.read
          .option("inferSchema", "true")
          .option("header", "true")
          .option("sep", columnSeparator)
          .csv(in)
          .coalesce(1)
          .write
          .mode(saveMode)
          .json(out)
      }
      .leftMap(thr =>
        JsonError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Reads json file
    *
    * @param path The json file path
    * @return DataFrame, otherwise Error
    */
  def readJson(path: String): Either[JsonError, DataFrame] = {
    Either
      .catchNonFatal {
        sparkSession.read.json(path)
      }
      .leftMap(thr =>
        JsonError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  def apply(in: String,
            out: String,
            separator: String,
            channel: Channel,
            saveMode: SaveMode): Either[DataHighwayError, List[Unit]] = {
    channel match {
      case ParquetJson =>
        handleParquetJsonChannel(in, out, saveMode)
      case CsvJson =>
        handleCsvJsonChannel(in, out, separator, saveMode)
      case _ =>
        throw new RuntimeException("Not suppose to happen !")
    }
  }

  /**
    * Converts csv files to json files
    *
    * @param in        The input csv path
    * @param out       The generated json file path
    * @param separator The csv columns separator
    * @param saveMode  The file saving mode
    * @return List[Unit], otherwise Error
    */
  private def handleCsvJsonChannel(
      in: String,
      out: String,
      separator: String,
      saveMode: SaveMode): Either[DataHighwayError, List[Unit]] = {
    for {
      folders <- FilesUtils.listFoldersRecursively(in)
      list <- folders
        .traverse(folder => {
          val suffix =
            FilesUtils.reversePathSeparator(folder).split("/").last
          saveCsvAsJson(folder, s"$out/$suffix", separator, saveMode)
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
    * @return List[Unit], otherwise Error
    */
  private def handleParquetJsonChannel(
      in: String,
      out: String,
      saveMode: SaveMode): Either[DataHighwayError, List[Unit]] = {
    for {
      folders <- FilesUtils.listFoldersRecursively(in)
      list <- folders
        .traverse(folder => {
          val suffix =
            FilesUtils.reversePathSeparator(folder).split("/").last
          saveParquetAsJson(folder, s"$out/$suffix", saveMode)
        })
        .leftMap(error =>
          JsonError(error.message, error.cause, error.stacktrace))
    } yield list
  }
}