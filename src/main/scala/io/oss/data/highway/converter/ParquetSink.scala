package io.oss.data.highway.converter

import io.oss.data.highway.model.DataHighwayError.ParquetError
import io.oss.data.highway.model.{
  CSV,
  Channel,
  CsvParquet,
  DataHighwayError,
  JSON,
  JsonParquet,
  PARQUET
}
import io.oss.data.highway.utils.{DataFrameUtils, FilesUtils}
import org.apache.spark.sql.{DataFrame, SaveMode}
import cats.implicits._

object ParquetSink {

  /**
    * Save a csv file as parquet
    * @param in The input csv path
    * @param out The generated parquet file path
    * @param saveMode The file saving mode
    * @return Unit if successful, otherwise Error
    */
  def saveCsvAsParquet(in: String,
                       out: String,
                       saveMode: SaveMode): Either[ParquetError, Unit] = {
    DataFrameUtils
      .loadDataFrame(in, CSV)
      .map(df => {
        df.write
          .mode(saveMode)
          .parquet(out)
      })
      .leftMap(thr =>
        ParquetError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Save a json file as parquet
    *
    * @param in       The input json path
    * @param out      The generated parquet file path
    * @param saveMode The file saving mode
    * @return Unit if successful, otherwise Error
    */
  def saveJsonAsParquet(in: String,
                        out: String,
                        saveMode: SaveMode): Either[ParquetError, Unit] = {
    DataFrameUtils
      .loadDataFrame(in, JSON)
      .map(df => {
        df.write
          .mode(saveMode)
          .parquet(out)
      })
      .leftMap(thr =>
        ParquetError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Reads parquet file
    *
    * @param path The parquet file path
    * @return DataFrame, otherwise Error
    */
  def readParquet(path: String): Either[ParquetError, DataFrame] = {
    DataFrameUtils
      .loadDataFrame(path, PARQUET)
      .leftMap(thr =>
        ParquetError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  def apply(in: String,
            out: String,
            channel: Channel,
            saveMode: SaveMode): Either[DataHighwayError, List[Unit]] = {
    channel match {
      case CsvParquet =>
        handleCsvParquetChannel(in, out, saveMode)
      case JsonParquet =>
        handleJsonParquetChannel(in, out, saveMode)
      case _ =>
        throw new RuntimeException("Not suppose to happen !")
    }
  }

  /**
    * Converts csv files to parquet files
    *
    * @param in              The input csv path
    * @param out             The generated parquet file path
    * @param saveMode        The file saving mode
    * @return List[Unit], otherwise Error
    */
  private def handleCsvParquetChannel(
      in: String,
      out: String,
      saveMode: SaveMode): Either[DataHighwayError, List[Unit]] = {
    for {
      folders <- FilesUtils.listFoldersRecursively(in)
      list <- folders
        .traverse(folder => {
          val suffix = FilesUtils.reversePathSeparator(folder).split("/").last
          saveCsvAsParquet(folder, s"$out/$suffix", saveMode)
        })
        .leftMap(error =>
          ParquetError(error.message, error.cause, error.stacktrace))
    } yield list
  }

  /**
    * Converts json files to parquet files
    *
    * @param in       The input json path
    * @param out      The generated parquet file path
    * @param saveMode The file saving mode
    * @return List[Unit], otherwise Error
    */
  private def handleJsonParquetChannel(
      in: String,
      out: String,
      saveMode: SaveMode): Either[DataHighwayError, List[Unit]] = {
    for {
      folders <- FilesUtils.listFoldersRecursively(in)
      list <- folders
        .traverse(folder => {
          val suffix = FilesUtils.reversePathSeparator(folder).split("/").last
          saveJsonAsParquet(folder, s"$out/$suffix", saveMode)
        })
        .leftMap(error =>
          ParquetError(error.message, error.cause, error.stacktrace))
    } yield list
  }
}
