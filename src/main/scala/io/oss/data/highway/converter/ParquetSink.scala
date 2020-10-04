package io.oss.data.highway.converter

import io.oss.data.highway.model.DataHighwayError.ParquetError
import io.oss.data.highway.model.{DataHighwayError, DataType}
import io.oss.data.highway.utils.{DataFrameUtils, FilesUtils}
import org.apache.spark.sql.SaveMode
import cats.implicits._
import io.oss.data.highway.configuration.SparkConfig

object ParquetSink {

  /**
    * Converts file to parquet
    *
    * @param in The input data path
    * @param out The generated parquet file path
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @param sparkConfig The Spark Configuration
    * @return Unit if successful, otherwise Error
    */
  def convertToParquet(in: String,
                       out: String,
                       saveMode: SaveMode,
                       inputDataType: DataType,
                       sparkConfig: SparkConfig): Either[ParquetError, Unit] = {
    DataFrameUtils(sparkConfig)
      .loadDataFrame(in, inputDataType)
      .map(df => {
        df.write
          .mode(saveMode)
          .parquet(out)
      })
      .leftMap(thr =>
        ParquetError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Converts files to parquet
    *
    * @param in The input data path
    * @param out The generated parquet file path
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @param sparkConfig The Spark Configuration
    * @return List[Unit], otherwise Error
    */
  def handleParquetChannel(
      in: String,
      out: String,
      saveMode: SaveMode,
      inputDataType: DataType,
      sparkConfig: SparkConfig): Either[DataHighwayError, List[Unit]] = {
    for {
      folders <- FilesUtils.listFoldersRecursively(in)
      list <- folders
        .traverse(folder => {
          val suffix = FilesUtils.reversePathSeparator(folder).split("/").last
          convertToParquet(folder,
                           s"$out/$suffix",
                           saveMode,
                           inputDataType,
                           sparkConfig)
        })
        .leftMap(error =>
          ParquetError(error.message, error.cause, error.stacktrace))
    } yield list
  }
}
