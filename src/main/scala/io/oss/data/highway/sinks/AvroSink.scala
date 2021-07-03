package io.oss.data.highway.sinks

import io.oss.data.highway.models.{AVRO, DataType, FileSystem, HDFS, Local}
import io.oss.data.highway.utils.{DataFrameUtils, FilesUtils, HdfsUtils}
import org.apache.spark.sql.SaveMode
import cats.implicits._
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger

import java.io.File

object AvroSink {

  val logger: Logger = Logger.getLogger(AvroSink.getClass.getName)

  /**
    * Converts file to avro
    *
    * @param in The input data path
    * @param out The generated avro file path
    * @param basePath The base path for input, output and processed folders
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @return a List of Path, otherwise an Error
    */
  def convertToAvro(
      in: String,
      out: String,
      basePath: String,
      saveMode: SaveMode,
      inputDataType: DataType
  ): Either[Throwable, Unit] = {
    DataFrameUtils
      .loadDataFrame(in, inputDataType)
      .map(df => {
        df.write
          .format(AVRO.extension)
          .mode(saveMode)
          .save(out)
        logger.info(
          s"Successfully converting '$inputDataType' data from input folder '$in' to '${AVRO.getClass.getName}' and " +
            s"store it under output folder '$out'."
        )
      })
  }

  /**
    * Converts files to avro
    *
    * @param in The input data path
    * @param out The generated avro file path
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @return List of List of Path, otherwise an Error
    */
  def handleAvroChannel(
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
    * @return
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
      list <-
        folders
          .filter(path => HdfsUtils.fs.listFiles(new Path(path), false).hasNext)
          .traverse(folder => {
            val suffix = folder.split("/").last
            convertToAvro(
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
    * @return
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
      list <-
        folders
          .filterNot(path => new File(path).listFiles.filter(_.isFile).toList.isEmpty)
          .traverse(folder => {
            val suffix = FilesUtils.reversePathSeparator(folder).split("/").last
            convertToAvro(
              folder,
              s"$out/$suffix",
              basePath,
              saveMode,
              inputDataType
            ).flatMap(_ => {
              FilesUtils.movePathContent(in, basePath)
            })
          })
      _ = FilesUtils.cleanup(in)
    } yield list
  }

}
