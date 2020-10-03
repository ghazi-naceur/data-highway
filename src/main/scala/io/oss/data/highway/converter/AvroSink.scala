package io.oss.data.highway.converter

import io.oss.data.highway.configuration.SparkConfig
import io.oss.data.highway.model.DataHighwayError.{AvroError, ParquetError}
import io.oss.data.highway.model.{
  AVRO,
  Channel,
  DataHighwayError,
  PARQUET,
  ParquetAvro
}
import io.oss.data.highway.utils.{DataFrameUtils, FilesUtils}
import org.apache.spark.sql.{DataFrame, SaveMode}
import cats.implicits._
import io.oss.data.highway.utils.Constants.AVRO_TYPE

object AvroSink {

  /**
    * Save a parquet file as avro
    * @param in The input parquet path
    * @param out The generated avro file path
    * @param saveMode The file saving mode
    * @param sparkConfig The Spark Configuration
    * @return Unit if successful, otherwise Error
    */
  def saveParquetAsAvro(in: String,
                        out: String,
                        saveMode: SaveMode,
                        sparkConfig: SparkConfig): Either[AvroError, Unit] = {
    DataFrameUtils(sparkConfig)
      .loadDataFrame(in, PARQUET)
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
    * @param in              The input parquet path
    * @param out             The generated avro file path
    * @param saveMode        The file saving mode
    * @param sparkConfig The Spark Configuration
    * @return List[Unit], otherwise Error
    */
  private def handleParquetAvroChannel(
      in: String,
      out: String,
      saveMode: SaveMode,
      sparkConfig: SparkConfig): Either[DataHighwayError, List[Unit]] = {
    for {
      folders <- FilesUtils.listFoldersRecursively(in)
      list <- folders
        .traverse(folder => {
          val suffix = FilesUtils.reversePathSeparator(folder).split("/").last
          saveParquetAsAvro(folder, s"$out/$suffix", saveMode, sparkConfig)
        })
        .leftMap(error =>
          AvroError(error.message, error.cause, error.stacktrace))
    } yield list
  }

  def apply(in: String,
            out: String,
            channel: Channel,
            saveMode: SaveMode,
            sparkConfig: SparkConfig): Either[DataHighwayError, List[Unit]] = {
    channel match {
      case ParquetAvro =>
        handleParquetAvroChannel(in, out, saveMode, sparkConfig)
      case _ =>
        throw new RuntimeException("Not suppose to happen !")
    }
  }

  /**
    * Reads avro file
    *
    * @param path The avro file path
    * @param sparkConfig The Spark Configuration
    * @return DataFrame, otherwise Error
    */
  def readAvro(path: String,
               sparkConfig: SparkConfig): Either[ParquetError, DataFrame] = {
    DataFrameUtils(sparkConfig)
      .loadDataFrame(path, AVRO)
      .leftMap(thr =>
        ParquetError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }
}
