package io.oss.data.highway.converter

import io.oss.data.highway.model.DataHighwayError.JsonError
import io.oss.data.highway.model.{DataHighwayError, DataType}
import io.oss.data.highway.utils.{DataFrameUtils, FilesUtils}
import org.apache.spark.sql.SaveMode
import cats.implicits._
import io.oss.data.highway.configuration.SparkConfig

object JsonSink {

  /**
    * Converts file to json
    *
    * @param in The input data path
    * @param out The generated json file path
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @param sparkConfig The Spark Configuration
    * @return Unit if successful, otherwise Error
    */
  def convertToJson(in: String,
                    out: String,
                    saveMode: SaveMode,
                    inputDataType: DataType,
                    sparkConfig: SparkConfig): Either[JsonError, Unit] = {
    DataFrameUtils(sparkConfig)
      .loadDataFrame(in, inputDataType)
      .map(df => {
        df.coalesce(1)
          .write
          .mode(saveMode)
          .json(out)
      })
      .leftMap(thr =>
        JsonError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Converts files to json
    *
    * @param in       The input data path
    * @param out      The generated json file path
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @param sparkConfig The Spark Configuration
    * @return List[Unit], otherwise Error
    */
  def handleJsonChannel(
      in: String,
      out: String,
      saveMode: SaveMode,
      inputDataType: DataType,
      sparkConfig: SparkConfig): Either[DataHighwayError, List[Unit]] = {
    for {
      folders <- FilesUtils.listFoldersRecursively(in)
      list <- folders
        .traverse(folder => {
          val suffix =
            FilesUtils.reversePathSeparator(folder).split("/").last
          convertToJson(folder,
                        s"$out/$suffix",
                        saveMode,
                        inputDataType,
                        sparkConfig)
        })
        .leftMap(error =>
          JsonError(error.message, error.cause, error.stacktrace))
    } yield list
  }
}
