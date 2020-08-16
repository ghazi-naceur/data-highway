package io.oss.data.highway.converter

import io.oss.data.highway.model.DataHighwayError.ParquetError
import io.oss.data.highway.model.{
  Channel,
  CsvParquet,
  DataHighwayError,
  JsonParquet
}
import io.oss.data.highway.utils.FilesUtils
import org.apache.spark.sql.{DataFrame, SaveMode}
import cats.implicits._
import io.oss.data.highway.utils.DataFrameUtils.sparkSession

object ParquetSink {

  /**
    * Save a csv file as parquet
    * @param in The input csv path
    * @param out The generated parquet file path
    * @param columnSeparator The column separator for each line in the csv file
    * @param saveMode The file saving mode
    * @return Unit if successful, otherwise Error
    */
  def saveCsvAsParquet(in: String,
                       out: String,
                       columnSeparator: String,
                       saveMode: SaveMode): Either[ParquetError, Unit] = {
    Either
      .catchNonFatal {
        // TODO Use DataFrameUtils.loadDataFrame
        sparkSession.read
          .option("inferSchema", "true")
          .option("header", "true")
          .option("sep", columnSeparator)
          .csv(in)
          .write
          .mode(saveMode)
          .parquet(out)
      }
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
    Either
      .catchNonFatal {
        sparkSession.read
          .json(in)
          .write
          .mode(saveMode)
          .parquet(out)
      }
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
    Either
      .catchNonFatal {
        sparkSession.read.parquet(path)
      }
      .leftMap(thr =>
        ParquetError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  def apply(in: String,
            out: String,
            columnSeparator: String,
            channel: Channel,
            saveMode: SaveMode): Either[DataHighwayError, List[Unit]] = {
    channel match {
      case CsvParquet =>
        handleCsvParquetChannel(in, out, columnSeparator, saveMode)
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
    * @param columnSeparator The column separator for each line in the csv file
    * @param saveMode        The file saving mode
    * @return List[Unit], otherwise Error
    */
  private def handleCsvParquetChannel(
      in: String,
      out: String,
      columnSeparator: String,
      saveMode: SaveMode): Either[DataHighwayError, List[Unit]] = {
    for {
      folders <- FilesUtils.listFoldersRecursively(in)
      list <- folders
        .traverse(folder => {
          val suffix = FilesUtils.reversePathSeparator(folder).split("/").last
          saveCsvAsParquet(folder, s"$out/$suffix", columnSeparator, saveMode)
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
