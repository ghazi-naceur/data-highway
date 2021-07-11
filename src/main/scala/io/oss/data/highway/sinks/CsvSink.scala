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
    * @return String, otherwise an Error
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
          s"Successfully converting '$inputDataType' data from input folder '$in' to '${CSV.getClass.getName}' and " +
            s"store it under output folder '$out'."
        )
        in
      })
  }

  /**
    * Converts files to csv
    *
    * @param in The input data path
    * @param out The output data path
    * @param saveMode The file saving mode
    * @param fileSystem The file system : It can be Local or HDFS
    * @param inputDataType The type of the input data
    * @return List of List of String, otherwise Error
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
    *
    * @param in The input data path
    * @param basePath The base path for input and output folders
    * @param out The output data path
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @return List of List of String, otherwise an Error
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
      res <- inputDataType match {
        case XLSX =>
          filtered
            .traverse(subFolder => {
              HdfsUtils
                .listFiles(subFolder)
                .traverse(file => {
                  val fileNameWithParentFolder = FilesUtils.getFileNameAndParentFolderFromPath(file)
                  convertToCsv(
                    file,
                    s"$out/$fileNameWithParentFolder",
                    basePath,
                    saveMode,
                    inputDataType
                  )
                })
                .flatMap(_ => {
                  HdfsUtils.movePathContent(subFolder, basePath)
                })
            })
        case _ =>
          filtered
            .traverse(subFolder => {
              val subFolderName = subFolder.split("/").last
              convertToCsv(
                subFolder,
                s"$out/$subFolderName",
                basePath,
                saveMode,
                inputDataType
              ).flatMap(_ => {
                HdfsUtils.movePathContent(subFolder, basePath)
              })
            })
      }
      _ = HdfsUtils.cleanup(in)
    } yield res
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
      folders <- FilesUtils.listFoldersRecursively(in)
      _ = logger.info("folders : " + folders)
      filtered <- FilesUtils.verifyNotEmpty(folders)
      res <- inputDataType match {
        case XLSX =>
          FilesUtils
            .listFiles(filtered)
            .traverse(files => {
              files.traverse(file => {
                val fileNameWithParentFolder =
                  FilesUtils.getFileNameAndParentFolderFromPath(file.toURI.getPath)
                convertToCsv(
                  file.toURI.getPath,
                  s"$out/$fileNameWithParentFolder",
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
          filtered.traverse(subFolder => {
            val subFolderName = FilesUtils.reversePathSeparator(subFolder).split("/").last
            convertToCsv(
              subFolder,
              s"$out/$subFolderName",
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
