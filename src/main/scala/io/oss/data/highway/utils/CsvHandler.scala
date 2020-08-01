package io.oss.data.highway.utils

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import cats.implicits._
import io.oss.data.highway.model.{
  Channel,
  DataHighwayError,
  JsonCsv,
  ParquetCsv
}
import io.oss.data.highway.model.DataHighwayError.CsvError

object CsvHandler {

  val ss: SparkSession = SparkSession
    .builder()
    .appName("csv-handler")
    .master("local[*]")
    .getOrCreate()
  ss.sparkContext.setLogLevel("WARN")

  /**
    * Save parquet file as csv
    *
    * @param in              The input parquet path
    * @param out             The generated csv file path
    * @param columnSeparator The column separator for each line in the csv file
    * @param saveMode        The file saving mode
    * @return Unit if successful, otherwise Error
    */
  def saveParquetAsCsv(in: String,
                       out: String,
                       columnSeparator: String,
                       saveMode: SaveMode): Either[CsvError, Unit] = {
    Either
      .catchNonFatal {
        ss.read
          .parquet(in)
          .coalesce(1)
          .write
          .mode(saveMode)
          .option("inferSchema", "true")
          .option("header", "true")
          .option("sep", columnSeparator)
          .csv(out)
      }
      .leftMap(thr => CsvError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Save json file as csv
    *
    * @param in              The input json path
    * @param out             The generated csv file path
    * @param columnSeparator The column separator for each line in the csv file
    * @param saveMode        The file saving mode
    * @return Unit if successful, otherwise Error
    */
  def saveJsonAsCsv(in: String,
                    out: String,
                    columnSeparator: String,
                    saveMode: SaveMode): Either[CsvError, Unit] = {
    Either
      .catchNonFatal {
        ss.read
          .json(in)
          .coalesce(1)
          .write
          .mode(saveMode)
          .option("inferSchema", "true")
          .option("header", "true")
          .option("sep", columnSeparator)
          .csv(out)
      }
      .leftMap(thr => CsvError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Reads csv file
    *
    * @param path The csv file path
    * @return DataFrame, otherwise Error
    */
  def readParquet(path: String,
                  columnSeparator: String): Either[CsvError, DataFrame] = {
    Either
      .catchNonFatal {
        ss.read
          .option("inferSchema", "true")
          .option("header", "true")
          .option("sep", columnSeparator)
          .csv(path)
      }
      .leftMap(thr => CsvError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  def apply(in: String,
            out: String,
            columnSeparator: String,
            channel: Channel,
            saveMode: SaveMode): Either[DataHighwayError, List[Unit]] = {
    channel match {
      case ParquetCsv =>
        handleParquetCsvChannel(in, out, columnSeparator, saveMode)
      case JsonCsv =>
        handleJsonCsvChannel(in, out, columnSeparator, saveMode)
      case _ =>
        throw new RuntimeException("Not supposed to happen !")
    }
  }

  /**
    * Converts parquet files to csv files
    *
    * @param in              The input parquet path
    * @param out             The generated csv file path
    * @param columnSeparator The column separator for each line in the csv file
    * @param saveMode        The file saving mode
    * @return List[Unit], otherwise Error
    */
  private def handleParquetCsvChannel(
      in: String,
      out: String,
      columnSeparator: String,
      saveMode: SaveMode): Either[DataHighwayError, List[Unit]] = {
    for {
      folders <- FilesUtils.listFoldersRecursively(in)
      list <- folders
        .traverse(folder => {
          val suffix = FilesUtils.reversePathSeparator(folder).split("/").last
          saveParquetAsCsv(folder, s"$out/$suffix", columnSeparator, saveMode)
        })
        .leftMap(error =>
          CsvError(error.message, error.cause, error.stacktrace))
    } yield list
  }

  /**
    * Converts json files to csv files
    *
    * @param in              The input json path
    * @param out             The generated csv file path
    * @param columnSeparator The column separator for each line in the csv file
    * @param saveMode        The file saving mode
    * @return List[Unit], otherwise Error
    */
  private def handleJsonCsvChannel(
      in: String,
      out: String,
      columnSeparator: String,
      saveMode: SaveMode): Either[DataHighwayError, List[Unit]] = {
    for {
      folders <- FilesUtils.listFoldersRecursively(in)
      list <- folders
        .traverse(folder => {
          val suffix = FilesUtils.reversePathSeparator(folder).split("/").last
          saveJsonAsCsv(folder, s"$out/$suffix", columnSeparator, saveMode)
        })
        .leftMap(error =>
          CsvError(error.message, error.cause, error.stacktrace))
    } yield list
  }
}
