package io.oss.data.highway.utils

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import cats.implicits._
import io.oss.data.highway.model.DataHighwayError
import io.oss.data.highway.model.DataHighwayError.{ParquetError, ReadFileError}

object ParquetHandler {

  val ss: SparkSession = SparkSession
    .builder()
    .appName("parquet-handler")
    .master("local[*]")
    .getOrCreate()
  ss.sparkContext.setLogLevel("WARN")

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
        ss.read
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
    * Reads parquet file
    * @param path The parquet file path
    * @return DataFrame, otherwise Error
    */
  def readParquet(path: String): Either[ParquetError, DataFrame] = {
    Either
      .catchNonFatal {
        ss.read.parquet(path)
      }
      .leftMap(thr =>
        ParquetError(thr.getMessage, thr.getCause, thr.getStackTrace))
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
  def apply(in: String,
            out: String,
            columnSeparator: String,
            saveMode: SaveMode): Either[DataHighwayError, List[Unit]] = {
    for {
      folders <- FilesUtils.listFoldersRecursively(in)
      list <- folders
        .traverse(folder => {
          val suffix = FilesUtils.reversePathSeparator(folder).split("/").last
          ParquetHandler
            .saveCsvAsParquet(folder,
                              s"$out/$suffix",
                              columnSeparator,
                              saveMode)
        })
        .leftMap(error =>
          ParquetError(error.message, error.cause, error.stacktrace))
    } yield list
  }
}
