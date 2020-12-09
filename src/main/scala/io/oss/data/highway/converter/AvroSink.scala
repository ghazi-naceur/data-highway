package io.oss.data.highway.converter

import io.oss.data.highway.configuration.SparkConfigs
import io.oss.data.highway.model.DataHighwayError.AvroError
import io.oss.data.highway.model.{AVRO, DataHighwayError, DataType}
import io.oss.data.highway.utils.{DataFrameUtils, FilesUtils}
import org.apache.spark.sql.SaveMode
import cats.implicits._
import io.oss.data.highway.utils.Constants.AVRO_TYPE
import org.apache.log4j.Logger

object AvroSink {

  val logger: Logger = Logger.getLogger(AvroSink.getClass.getName)

  /**
    * Converts file to avro
    *
    * @param in The input data path
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
                    sparkConfig: SparkConfigs): Either[AvroError, Unit] = {
    DataFrameUtils(sparkConfig)
      .loadDataFrame(in, inputDataType)
      .map(df => {
        df.write
          .format(AVRO_TYPE)
          .mode(saveMode)
          .save(out)
        logger.info(
          s"Successfully converting '$inputDataType' data from input folder '$in' to '${AVRO.getClass}' and store it under output folder '$out'.")
      })
      .leftMap(thr =>
        AvroError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Converts files to avro
    *
    * @param in The input data path
    * @param out The generated avro file path
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @param sparkConfig The Spark Configuration
    * @return List[Unit], otherwise Error
    */
  def handleAvroChannel(
      in: String,
      out: String,
      saveMode: SaveMode,
      inputDataType: DataType,
      sparkConfig: SparkConfigs): Either[DataHighwayError, List[Unit]] = {
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
}
