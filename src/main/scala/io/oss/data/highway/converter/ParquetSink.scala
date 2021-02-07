package io.oss.data.highway.converter

import io.oss.data.highway.model.{DataType, PARQUET}
import io.oss.data.highway.utils.{DataFrameUtils, FilesUtils}
import org.apache.spark.sql.SaveMode
import cats.implicits._
import io.oss.data.highway.configuration.SparkConfigs
import org.apache.log4j.Logger

import java.io.File
import java.nio.file.Path

object ParquetSink {

  val logger: Logger = Logger.getLogger(ParquetSink.getClass.getName)

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
  def convertToParquet(
      in: String, //app/data/input/mock-data-2
      out: String, //app/data/output/mock-data-2
      basePath: String, //app/data
      saveMode: SaveMode,
      inputDataType: DataType,
      sparkConfig: SparkConfigs): Either[Throwable, List[Path]] = {
    DataFrameUtils(sparkConfig)
      .loadDataFrame(in, inputDataType)
      .map(df => {
        df.write
          .mode(saveMode)
          .parquet(out)
        logger.info(
          s"Successfully converting '$inputDataType' data from input folder '$in' to '${PARQUET.getClass.getName}' and store it under output folder '$out'.")
      })
      .flatMap(_ => {
        FilesUtils.movePathContent(in, basePath)
      })
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
  def handleParquetChannel(in: String,
                           out: String,
                           saveMode: SaveMode,
                           inputDataType: DataType,
                           sparkConfig: SparkConfigs) = {
    val basePath = new File(in).getParent
    for {
      folders <- FilesUtils.listFoldersRecursively(in)
      _ = logger.info("folders : " + folders)
      list <- folders
        .filterNot(path =>
          new File(path).listFiles.filter(_.isFile).toList.isEmpty)
        .traverse(folder => {
          val suffix = FilesUtils.reversePathSeparator(folder).split("/").last
          convertToParquet(folder,
                           s"$out/$suffix",
                           basePath,
                           saveMode,
                           inputDataType,
                           sparkConfig)
        })
      _ = FilesUtils.deleteFolder(in)
    } yield list
  }
}
