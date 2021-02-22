package io.oss.data.highway.converter

import io.oss.data.highway.model.{DataType, PARQUET}
import io.oss.data.highway.utils.{DataFrameUtils, FilesUtils}
import org.apache.spark.sql.SaveMode
import cats.implicits._
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
    * @param basePath The base path for input, output and processed folders
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @return a List of Path, otherwise an Error
    */
  def convertToParquet(
      in: String,
      out: String,
      basePath: String,
      saveMode: SaveMode,
      inputDataType: DataType): Either[Throwable, List[Path]] = {
    DataFrameUtils
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
    * @return List of List of Path, otherwise an Error
    */
  def handleParquetChannel(
      in: String,
      out: String,
      saveMode: SaveMode,
      inputDataType: DataType): Either[Throwable, List[List[Path]]] = {
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
                           inputDataType)
        })
      _ = FilesUtils.deleteFolder(in)
    } yield list
  }
}
