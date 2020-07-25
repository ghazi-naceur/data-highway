package io.oss.data.highway.utils

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import cats.implicits._
import io.oss.data.highway.model.DataHighwayError.ParquetError

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
    *
    * @param in The input csv path
    * @param out The generated parquet file path
    * @param columnSeparator The column separator for each line in the csv file
    * @param saveMode The file saving mode
    * @return
    */
  def apply(in: String,
            out: String,
            columnSeparator: String,
            saveMode: SaveMode): Either[ParquetError, Unit] = {
    for {
      _ <- ParquetHandler.saveCsvAsParquet(in, out, columnSeparator, saveMode)
      df <- ParquetHandler.readParquet(out)
    } yield df.show(false)
  }
}