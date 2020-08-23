package io.oss.data.highway.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import cats.syntax.either._
import io.oss.data.highway.model.{CSV, DataType, JSON, PARQUET}
import io.oss.data.highway.utils.Constants.{MASTER_URL, SEPARATOR}

object DataFrameUtils {

  val sparkSession: SparkSession = {
    val ss = SparkSession
      .builder()
      .appName("handler")
      .master(MASTER_URL)
      .getOrCreate()
    ss.sparkContext.setLogLevel("WARN")
    ss
  }

  def loadDataFrame(in: String,
                    dataType: DataType): Either[Throwable, DataFrame] = {
    Either.catchNonFatal {
      dataType match {
        case JSON =>
          sparkSession.read
            .json(in)
        case CSV =>
          sparkSession.read
            .option("inferSchema", "true")
            .option("header", "true")
            .option("sep", SEPARATOR)
            .csv(in)
        case PARQUET =>
          sparkSession.read
            .parquet(in)
      }
    }
  }
}
