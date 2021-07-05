package io.oss.data.highway.sinks

import io.oss.data.highway.models.{DataType, FileSystem, HDFS, Local, PARQUET}
import io.oss.data.highway.utils.{DataFrameUtils, FilesUtils, HdfsUtils}
import org.apache.spark.sql.SaveMode
import cats.implicits._
import org.apache.log4j.Logger

import java.io.File

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
      inputDataType: DataType
  ): Either[Throwable, String] = {
    DataFrameUtils
      .loadDataFrame(in, inputDataType)
      .map(df => {
        df.write
          .mode(saveMode)
          .parquet(out)
        logger.info(
          s"Successfully converting '$inputDataType' data from input folder '$in' to '${PARQUET.getClass.getName}' and " +
            s"store it under output folder '$out'."
        )
        in
      })
  }

  /**
    * Converts files to parquet
    *
    * @param in The input data path
    * @param out The generated parquet file path
    * @param saveMode The file saving mode
    * @param fileSystem The file system : It can be *Local* or *HDFS*
    * @param inputDataType The type of the input data
    * @return List of List of Path, otherwise an Error
    */
  def handleParquetChannel(
      in: String,
      out: String,
      saveMode: SaveMode,
      fileSystem: FileSystem,
      inputDataType: DataType
  ): Either[Throwable, List[List[String]]] = {
    val basePath = new File(in).getParent

    fileSystem match {
      case Local =>
        handleLocalFS(in, basePath, out, saveMode, inputDataType)
      case HDFS =>
        handleHDFS(in, basePath, out, saveMode, inputDataType)
    }
  }

  /**
    * Handles data conversion for HDFS
    * @param in The input data path
    * @param basePath The base path for input and output folders
    * @param out The generated parquet file path
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @return List of List of Path, otherwise an Error
    */
  private def handleHDFS(
      in: String,
      basePath: String,
      out: String,
      saveMode: SaveMode,
      inputDataType: DataType
  ): Either[Throwable, List[List[String]]] = {
    for {
      folders <- HdfsUtils.listFolders(in)
      _ = logger.info("folders : " + folders)
      filtered <- HdfsUtils.verifyNotEmpty(folders)
      list <-
        filtered
          .traverse(folder => {
            val suffix = folder.split("/").last
            convertToParquet(
              folder,
              s"$out/$suffix",
              basePath,
              saveMode,
              inputDataType
            ).flatMap(_ => {
              HdfsUtils.movePathContent(folder, basePath)
            })
          })
      _ = HdfsUtils.cleanup(in)
    } yield list
  }

  /**
    * Handles data conversion for Local File System
    * @param in The input data path
    * @param basePath The base path for input and output folders
    * @param out The generated parquet file path
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @return List of List of Path, otherwise an Error
    */
  private def handleLocalFS(
      in: String,
      basePath: String,
      out: String,
      saveMode: SaveMode,
      inputDataType: DataType
  ): Either[Throwable, List[List[String]]] = {
    for {
      folders <- FilesUtils.listFoldersRecursively(in)
      _ = logger.info("folders : " + folders)
      filtered <- FilesUtils.verifyNotEmpty(folders)
      list <-
        filtered
          .traverse(folder => {
            val suffix = FilesUtils.reversePathSeparator(folder).split("/").last
            convertToParquet(
              folder,
              s"$out/$suffix",
              basePath,
              saveMode,
              inputDataType
            ).flatMap(subInputFolder => {
              FilesUtils.movePathContent(subInputFolder, basePath, inputDataType)
            })
          })
      _ = FilesUtils.cleanup(in)
    } yield list
  }
}
