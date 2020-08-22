package io.oss.data.highway.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import cats.syntax.either._
import io.oss.data.highway.model.{CSV, DataType, JSON, PARQUET}

object DataFrameUtils {

  // TODO externalize "local[*]"
  val sparkSession: SparkSession = {
    val ss = SparkSession
      .builder()
      .appName("handler")
      .master("local[*]")
      .getOrCreate()
    ss.sparkContext.setLogLevel("WARN")
    ss
  }

  // TODO use this method
  def loadDataFrame(in: String,
                    dataType: DataType): Either[Throwable, DataFrame] = {
    Either.catchNonFatal {
      dataType match {
        case JSON =>
          sparkSession.read
            .json(in)
        case CSV =>
          // TODO Add options for csv data type
          sparkSession.read
            .csv(in)
        case PARQUET =>
          sparkSession.read
            .parquet(in)
      }
    }
  }
}
