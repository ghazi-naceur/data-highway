package io.oss.data.highway.utils

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import cats.implicits._
import io.oss.data.highway.model.DataHighwayError.ParquetError

object ParquetHandler {

  val ss: SparkSession = SparkSession.builder().appName("parquet-handler").master("local[*]").getOrCreate()
  ss.sparkContext.setLogLevel("WARN")

  def saveCsvAsParquet(in: String, out: String, columnSeparator: String, saveMode: SaveMode): Either[ParquetError, Unit] = {
    Either.catchNonFatal {
      ss.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("sep", columnSeparator)
        .csv(in)
        .write.mode(saveMode).parquet(out)
    }.leftMap(thr => ParquetError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  def readParquet(path: String): Either[ParquetError, DataFrame] = {
    Either.catchNonFatal {
      ss.read.parquet(path)
    }.leftMap(thr => ParquetError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }
}
