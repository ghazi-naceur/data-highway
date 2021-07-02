package io.oss.data.highway.sinks

import io.oss.data.highway.models.{DataType, FileSystem, HDFS, Local, PARQUET}
import io.oss.data.highway.utils.{DataFrameUtils, FilesUtils, HdfsUtils}
import org.apache.spark.sql.SaveMode
import cats.implicits._
import org.apache.hadoop.fs.{LocatedFileStatus, Path, RemoteIterator}
import org.apache.log4j.Logger

import java.io.File
import java.net.URI

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
      fileSystem: FileSystem,
      inputDataType: DataType
  ): Either[Throwable, List[String]] = {
    DataFrameUtils
      .loadDataFrame(in, inputDataType)
      .map(df => {
        df.write
          .mode(saveMode)
          .parquet(out)
        logger.info(
          s"Successfully converting '$inputDataType' data from input folder '$in' to '${PARQUET.getClass.getName}' and store it under output folder '$out'."
        )
      })
      .flatMap(_ => {
        FilesUtils.movePathContent(in, basePath, fileSystem)
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
      fileSystem: FileSystem,
      inputDataType: DataType
  ): Either[Throwable, List[List[String]]] = {
    val basePath = new File(in).getParent

    fileSystem match {
      case Local =>
        for {
          folders <- FilesUtils.listFoldersRecursively(in)
          _ = logger.info("folders : " + folders)
          list <-
            folders
              .filterNot(path => new File(path).listFiles.filter(_.isFile).toList.isEmpty)
              .traverse(folder => {
                val suffix = FilesUtils.reversePathSeparator(folder).split("/").last
                convertToParquet(
                  folder,
                  s"$out/$suffix",
                  basePath,
                  saveMode,
                  fileSystem,
                  inputDataType
                )
              })
          _ = FilesUtils.deleteFolder(in)
        } yield list
      case HDFS =>
        for {
          folders <- HdfsUtils.listFolders(in)
          _ = logger.info("folders : " + folders)
          list <-
            folders
              .filter(path => HdfsUtils.fs.listFiles(new Path(path), false).hasNext)
              .traverse(folder => {
                val suffix = FilesUtils.reversePathSeparator(folder).split("/").last
                convertToParquet(
                  folder,
                  s"$out/$suffix",
                  basePath,
                  saveMode,
                  fileSystem,
                  inputDataType
                )
              })
          _ = HdfsUtils.rmdir(in)
        } yield list
    }
  }
}
