package io.oss.data.highway.converter

import io.oss.data.highway.configuration.SparkConfig
import io.oss.data.highway.model.DataHighwayError.AvroError
import io.oss.data.highway.model.{DataHighwayError, DataType}
import io.oss.data.highway.utils.{DataFrameUtils, FilesUtils}
import org.apache.spark.sql.SaveMode
import cats.implicits._
import io.oss.data.highway.utils.Constants.AVRO_TYPE

object AvroSink {

  /**
    * Converts a file to avro
    * @param in The input parquet path
    * @param out The generated avro file path
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @param sparkConfig The Spark Configuration
    * @return Unit if successful, otherwise Error
    */
  def convertToAvro(in: String,
                    out: String,
                    saveMode: SaveMode,
                    inputDataType: DataType,
                    sparkConfig: SparkConfig): Either[AvroError, Unit] = {
    DataFrameUtils(sparkConfig)
      .loadDataFrame(in, inputDataType)
      .map(df => {
        df.write
          .format(AVRO_TYPE)
          .mode(saveMode)
          .save(out)
      })
      .leftMap(thr =>
        AvroError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Converts parquet files to avro files
    *
    * @param in The input parquet path
    * @param out The generated avro file path
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @param sparkConfig The Spark Configuration
    * @return List[Unit], otherwise Error
    */
  private def handleAvroChannel(
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
          convertToAvro(folder,
                        s"$out/$suffix",
                        saveMode,
                        inputDataType,
                        sparkConfig)
        })
        .leftMap(error =>
          AvroError(error.message, error.cause, error.stacktrace))
    } yield list
  }

  def apply(in: String,
            out: String,
            saveMode: SaveMode,
            inputDataType: DataType,
            sparkConfig: SparkConfig): Either[DataHighwayError, List[Unit]] = {
    handleAvroChannel(in, out, saveMode, inputDataType, sparkConfig)
  }
}
