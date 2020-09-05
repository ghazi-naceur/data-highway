package io.oss.data.highway.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import cats.syntax.either._
import io.oss.data.highway.configuration.SparkConfig
import io.oss.data.highway.model.{CSV, DataType, JSON, PARQUET}
import io.oss.data.highway.utils.Constants.{MASTER_URL, SEPARATOR}

case class DataFrameUtils(sparkConf: SparkConfig) {

  val sparkSession: SparkSession = {
    val ss = SparkSession
      .builder()
      .appName(sparkConf.appName)
      .master(sparkConf.masterUrl)
      .getOrCreate()
    ss.sparkContext.setLogLevel(sparkConf.logLevel.value)
    ss
  }

  /**
    * Loads a dataframe
    * @param in The input path
    * @param dataType a datatype to be load : CSV, JSON or PARQUET
    * @return A DataFrame, otherwise an Error
    */
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
