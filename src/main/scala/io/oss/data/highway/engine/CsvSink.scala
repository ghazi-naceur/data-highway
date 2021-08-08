package io.oss.data.highway.engine

import java.io.File
import io.oss.data.highway.models._
import io.oss.data.highway.utils.Constants._
import io.oss.data.highway.utils.{DataFrameUtils, FilesUtils, HdfsUtils}
import org.apache.spark.sql.SaveMode
import cats.implicits._
import io.oss.data.highway.models.{DataType, HDFS, Local, Storage}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger

object CsvSink extends HdfsUtils {

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
    * @param storage The file system storage : It can be Local or HDFS
    * @param inputDataType The type of the input data
    * @return List of List of String, otherwise Error
    */
  def handleCsvChannel(
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
    * @param fs The provided File System
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
      res <- inputDataType match {
        case XLSX =>
          filtered
            .traverse(subFolder => {
              HdfsUtils
                .listFiles(fs, subFolder)
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
                  HdfsUtils.movePathContent(fs, subFolder, basePath)
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
                HdfsUtils.movePathContent(fs, subFolder, basePath)
              })
            })
      }
      _ = HdfsUtils.cleanup(fs, in)
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
      folders <- FilesUtils.listNonEmptyFoldersRecursively(in)
      _ = logger.info("folders : " + folders)
      filtered <- FilesUtils.filterNonEmptyFolders(folders)
      res <- inputDataType match {
        case XLSX =>
          FilesUtils
            .listFiles(filtered)
            .traverse(files => {
              files
                .traverse(file => {
                  val fileNameWithParentFolder =
                    FilesUtils.getFileNameAndParentFolderFromPath(file.toURI.getPath)
                  convertToCsv(
                    file.toURI.getPath,
                    s"$out/$fileNameWithParentFolder",
                    basePath,
                    saveMode,
                    inputDataType
                  ).flatMap(subInputFolder => {
                    FilesUtils
                      .movePathContent(
                        subInputFolder,
                        s"$basePath/processed/${new File(
                          subInputFolder
                        ).getParentFile.toURI.getPath.split("/").takeRight(1).mkString("/")}"
                      )
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
              FilesUtils.movePathContent(subInputFolder, s"$basePath/processed")
            })
          })
      }
      _ = FilesUtils.cleanup(in)
    } yield res
  }
}
