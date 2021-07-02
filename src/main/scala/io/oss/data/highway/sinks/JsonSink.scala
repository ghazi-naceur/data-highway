package io.oss.data.highway.sinks

import io.oss.data.highway.models.{DataType, FileSystem, JSON}
import io.oss.data.highway.utils.{DataFrameUtils, FilesUtils}
import org.apache.spark.sql.SaveMode
import cats.implicits._
import org.apache.log4j.Logger

import java.io.File

object JsonSink {

  val logger: Logger = Logger.getLogger(JsonSink.getClass.getName)

  /**
    * Converts file to json
    *
    * @param in The input data path
    * @param out The generated json file path
    * @param basePath The base path for input, output and processed folders
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @return a List of Path, otherwise an Error
    */
  def convertToJson(
      in: String,
      out: String,
      basePath: String,
      saveMode: SaveMode,
      fileSystem: FileSystem,
      inputDataType: DataType
  ): Either[Throwable, List[String]] = {
    DataFrameUtils
      .loadDataFrame(in, inputDataType)
      .map(df => {
        df.coalesce(1)
          .write
          .mode(saveMode)
          .json(out)
        logger.info(
          s"Successfully converting '$inputDataType' data from input folder '$in' to '${JSON.getClass.getName}' and store it under output folder '$out'."
        )
      })
      .flatMap(_ => FilesUtils.movePathContent(in, basePath, fileSystem))
  }

  /**
    * Converts files to json
    *
    * @param in       The input data path
    * @param out      The generated json file path
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @return List of List of Path, otherwise an Error
    */
  def handleJsonChannel(
      in: String,
      out: String,
      saveMode: SaveMode,
      fileSystem: FileSystem,
      inputDataType: DataType
  ): Either[Throwable, List[List[String]]] = {
    val basePath = new File(in).getParent
    for {
      folders <- FilesUtils.listFoldersRecursively(in)
      list <-
        folders
          .filterNot(path => new File(path).listFiles.filter(_.isFile).toList.isEmpty)
          .traverse(folder => {
            val suffix =
              FilesUtils.reversePathSeparator(folder).split("/").last
            convertToJson(folder, s"$out/$suffix", basePath, saveMode, fileSystem, inputDataType)
          })
      _ = FilesUtils.deleteFolder(in)
    } yield list
  }
}
