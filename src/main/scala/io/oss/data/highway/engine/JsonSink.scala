package io.oss.data.highway.engine

import io.oss.data.highway.models.{DataType, Storage, HDFS, JSON, Local}
import io.oss.data.highway.utils.{DataFrameUtils, FilesUtils, HdfsUtils}
import org.apache.spark.sql.SaveMode
import cats.implicits._
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger

import java.io.File

object JsonSink extends HdfsUtils {

  val logger: Logger = Logger.getLogger(JsonSink.getClass.getName)

  /**
    * Converts file to json
    *
    * @param in The input data path
    * @param out The generated json file path
    * @param basePath The base path for input, output and processed folders
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @return String, otherwise an Error
    */
  def convertToJson(
      in: String,
      out: String,
      basePath: String,
      saveMode: SaveMode,
      inputDataType: DataType
  ): Either[Throwable, String] = {
    DataFrameUtils
      .loadDataFrame(inputDataType, in)
      .map(df => {
        df.coalesce(1)
          .write
          .mode(saveMode)
          .json(out)
        logger.info(
          s"Successfully converting '$inputDataType' data from input folder '$in' to '${JSON.getClass.getName}' and " +
            s"store it under output folder '$out'."
        )
        in
      })
  }

  /**
    * Converts files to json
    *
    * @param in The input data path
    * @param out The output data path
    * @param saveMode The file saving mode
    * @param storage The file system storage : It can be Local or HDFS
    * @param inputDataType The type of the input data
    * @return List of List of Path, otherwise an Error
    */
  def handleJsonChannel(
      in: String,
      out: String,
      saveMode: SaveMode,
      storage: Storage,
      inputDataType: DataType
  ): Either[Throwable, List[List[String]]] = {
    val basePath = new File(in).getParent

    storage match {
      case Local =>
        handleLocalFS(in, basePath, out, saveMode, inputDataType)
      case HDFS =>
        handleHDFS(in, basePath, out, saveMode, inputDataType, fs)
    }
  }

  /**
    * Handles data conversion for HDFS
    *
    * @param in The input data path
    * @param basePath The base path for input and output folders
    * @param out The output data path
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @param fs The provided File System storage
    * @return List of List of String, otherwise an Error
    */
  private def handleHDFS(
      in: String,
      basePath: String,
      out: String,
      saveMode: SaveMode,
      inputDataType: DataType,
      fs: FileSystem
  ): Either[Throwable, List[List[String]]] = {
    for {
      folders <- HdfsUtils.listFolders(fs, in)
      _ = logger.info("folders : " + folders)
      filtered <- HdfsUtils.filterNonEmptyFolders(fs, folders)
      list <-
        filtered
          .traverse(folder => {
            val suffix = folder.split("/").last
            convertToJson(
              folder,
              s"$out/$suffix",
              basePath,
              saveMode,
              inputDataType
            ).flatMap(_ => {
              HdfsUtils.movePathContent(fs, folder, basePath)
            })
          })
      _ = HdfsUtils.cleanup(fs, in)
    } yield list
  }

  /**
    * Handles data conversion for Local File System
    *
    * @param in The input data path
    * @param basePath The base path for input and output folders
    * @param out The output data path
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @return List of List of String, otherwise an Error
    */
  private def handleLocalFS(
      in: String,
      basePath: String,
      out: String,
      saveMode: SaveMode,
      inputDataType: DataType
  ): Either[Throwable, List[List[String]]] = {
    for {
      folders <- FilesUtils.listNonEmptyFoldersRecursively(in)
      _ = logger.info("folders : " + folders)
      filtered <- FilesUtils.filterNonEmptyFolders(folders)
      list <-
        filtered
          .traverse(folder => {
            val suffix = FilesUtils.reversePathSeparator(folder).split("/").last
            convertToJson(
              folder,
              s"$out/$suffix",
              basePath,
              saveMode,
              inputDataType
            ).flatMap(subInputFolder => {
              FilesUtils.movePathContent(subInputFolder, s"$basePath/processed")
            })
          })
      _ = FilesUtils.cleanup(in)
    } yield list
  }
}