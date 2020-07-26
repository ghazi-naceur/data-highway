package io.oss.data.highway.utils

import io.oss.data.highway.model.DataHighwayError
import io.oss.data.highway.model.DataHighwayError.JsonError
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import cats.implicits._

object JsonHandler {

  val ss: SparkSession = SparkSession
    .builder()
    .appName("json-handler")
    .master("local[*]")
    .getOrCreate()
  ss.sparkContext.setLogLevel("WARN")

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
        ss.read
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
    * Reads json file
    *
    * @param path The json file path
    * @return DataFrame, otherwise Error
    */
  def readJson(path: String): Either[JsonError, DataFrame] = {
    Either
      .catchNonFatal {
        ss.read.json(path)
      }
      .leftMap(thr =>
        JsonError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Converts parquet files to json files
    *
    * @param in       The input parquet path
    * @param out      The generated json file path
    * @param saveMode The file saving mode
    * @return List[Unit], otherwise Error
    */
  def apply(in: String,
            out: String,
            saveMode: SaveMode): Either[DataHighwayError, List[Unit]] = {
    for {
      folders <- FilesUtils.listFoldersRecursively(in)
      list <- folders
        .traverse(folder => {
          val suffix = FilesUtils.reversePathSeparator(folder).split("/").last
          saveParquetAsJson(folder, s"$out/$suffix", saveMode)
        })
        .leftMap(error =>
          JsonError(error.message, error.cause, error.stacktrace))
    } yield list
  }
}
