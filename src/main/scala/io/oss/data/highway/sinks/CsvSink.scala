package io.oss.data.highway.sinks

import java.io.File
import io.oss.data.highway.models._
import io.oss.data.highway.utils.Constants._
import io.oss.data.highway.utils.{DataFrameUtils, FilesUtils, HdfsUtils}
import org.apache.spark.sql.SaveMode
import cats.implicits._
import org.apache.log4j.Logger

object CsvSink {

  val logger: Logger = Logger.getLogger(CsvSink.getClass.getName)

  /**
    * Converts file to csv
    *
    * @param in The input data path
    * @param out The generated csv file path
    * @param basePath The base path for input, output and processed folders
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @return a List of Path, otherwise an Error
    */
  def convertToCsv(
      in: String,
      out: String,
      basePath: String,
      saveMode: SaveMode,
      inputDataType: DataType
  ): Either[Throwable, String] = {
    DataFrameUtils
      .loadDataFrame(in, inputDataType)
      .map(df => {
        df.coalesce(1)
          .write
          .mode(saveMode)
          .option("inferSchema", "true")
          .option("header", "true")
          .option("sep", SEPARATOR)
          .csv(out)
        logger.info(
          s"Successfully converting '$inputDataType' data from input folder '$in' to '${CSV.getClass.getName}' and store it under output folder '$out'."
        )
        in
      })
  }

  /**
    * Converts files to csv
    *
    * @param in The input data path
    * @param out The generated csv file path
    * @param saveMode The file saving mode
    * @param fileSystem The file system : It can be *Local* or *HDFS*
    * @param inputDataType The type of the input data
    * @return List of List of Path, otherwise Error
    */
  def handleCsvChannel(
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
      res <- inputDataType match {
        case XLSX =>
          folders
            .filter(path =>
              HdfsUtils.fs.listFiles(new org.apache.hadoop.fs.Path(path), false).hasNext
            )
            .traverse(folder => {
              HdfsUtils
                .listFiles(folder)
                .traverse(file => {
                  val suffix = file
                    .split("/")
                    .takeRight(2)
                    .mkString("/")
                    .replace(".xlsx", "")
                  convertToCsv(
                    file,
                    s"$out/$suffix",
                    basePath,
                    saveMode,
                    inputDataType
                  )
                })
                .flatMap(_ => {
                  HdfsUtils.movePathContent(folder, basePath)
                })
            })
        case _ =>
          folders
            .filter(path =>
              HdfsUtils.fs.listFiles(new org.apache.hadoop.fs.Path(path), false).hasNext
            )
            .traverse(folder => {
              val suffix = folder.split("/").last
              convertToCsv(
                folder,
                s"$out/$suffix",
                basePath,
                saveMode,
                inputDataType
              ).flatMap(_ => {
                HdfsUtils.movePathContent(folder, basePath)
              })
            })
      }
      _ = HdfsUtils.cleanup(in)
    } yield res
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
      list <- Either.catchNonFatal {
        folders.filterNot(path => new File(path).listFiles.filter(_.isFile).toList.isEmpty)
      }
      res <- inputDataType match {
        case XLSX =>
          val files = Either.catchNonFatal {
            list.flatMap(subfolder => {
              new File(subfolder).listFiles
            })
          }
          files
            .traverse(folder => {
              folder.traverse(fi => {
                val suffix =
                  FilesUtils
                    .reversePathSeparator(fi.toURI.getPath)
                    .split("/")
                    .takeRight(2)
                    .mkString("/")
                    .replace(".xlsx", "")
                convertToCsv(
                  fi.toURI.getPath,
                  s"$out/$suffix",
                  basePath,
                  saveMode,
                  inputDataType
                ).flatMap(subInputFolder => {
                  FilesUtils.movePathContent(subInputFolder, basePath, inputDataType)
                })
              })
            })
            .flatten
        case _ =>
          list.traverse(folder => {
            val suffix = FilesUtils.reversePathSeparator(folder).split("/").last
            convertToCsv(
              folder,
              s"$out/$suffix",
              basePath,
              saveMode,
              inputDataType
            ).flatMap(subInputFolder => {
              FilesUtils.movePathContent(subInputFolder, basePath, inputDataType)
            })
          })
      }
      _ = FilesUtils.cleanup(in)
    } yield res
  }
}
