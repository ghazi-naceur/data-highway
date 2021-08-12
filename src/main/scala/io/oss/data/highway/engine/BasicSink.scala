package io.oss.data.highway.engine

import java.io.File
import io.oss.data.highway.models._
import io.oss.data.highway.utils.{DataFrameUtils, FilesUtils, HdfsUtils}
import org.apache.spark.sql.SaveMode
import cats.implicits._
import io.oss.data.highway.models
import io.oss.data.highway.models.DataHighwayError.DataHighwayFileError
import io.oss.data.highway.models.{DataType, HDFS, Local, Storage}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger

object BasicSink extends HdfsUtils {

  val logger: Logger = Logger.getLogger(BasicSink.getClass.getName)

  /**
    * Converts file
    *
    * @param inputDataType The input data type path
    * @param inputPath The input path
    * @param outputDataType The input data type path
    * @param outputPath The input path
    * @param basePath The base path for input, output and processed folders
    * @param saveMode The file saving mode
    * @return Path as String, otherwise an Throwable
    */
  def convert(
      inputDataType: DataType,
      inputPath: String,
      outputDataType: DataType,
      outputPath: String,
      basePath: String,
      saveMode: SaveMode
  ): Either[Throwable, String] = {
    DataFrameUtils
      .loadDataFrame(inputDataType, inputPath)
      .map(df => {
        DataFrameUtils.saveDataFrame(df, outputDataType, outputPath, saveMode)
        inputPath
      })
  }

  /**
    * Converts files
    *
    * @param input The input DataHighway File Entity
    * @param output The output DataHighway File Entity
    * @param storage The file system storage : It can be Local or HDFS
    * @param saveMode The file saving mode
    * @return List of List of Path as String, otherwise Throwable
    */
  def handleChannel(
      input: models.File,
      output: models.File,
      storage: Option[Storage],
      saveMode: SaveMode
  ): Either[Throwable, List[List[String]]] = {
    val basePath = new File(input.path).getParent
    storage match {
      case Some(value) =>
        value match {
          case Local =>
            handleLocalFS(
              input,
              output,
              basePath,
              saveMode
            )
          case HDFS =>
            handleHDFS(
              input,
              output,
              basePath,
              saveMode,
              fs
            )
        }
      case None =>
        Left(
          DataHighwayFileError(
            "MissingFileSystemStorage",
            new RuntimeException("Missing 'storage' field"),
            Array[StackTraceElement]()
          )
        )
    }

  }

  /**
    * Handles data conversion for HDFS
    *
    * @param input The input DataHighway File Entity
    * @param output The output DataHighway File Entity
    * @param basePath The base path for input, output and processed folders
    * @param saveMode The file saving mode
    * @param fs The provided File System
    * @return List of List of Path as String, otherwise Throwable
    */
  private def handleHDFS(
      input: models.File,
      output: models.File,
      basePath: String,
      saveMode: SaveMode,
      fs: FileSystem
  ): Either[Throwable, List[List[String]]] = {
    for {
      folders <- HdfsUtils.listFolders(fs, input.path)
      _ = logger.info("folders : " + folders)
      filtered <- HdfsUtils.filterNonEmptyFolders(fs, folders)
      res <- input.dataType match {
        case XLSX =>
          filtered
            .traverse(subFolder => {
              HdfsUtils
                .listFiles(fs, subFolder)
                .traverse(file => {
                  val fileNameWithParentFolder = FilesUtils.getFileNameAndParentFolderFromPath(file)
                  convert(
                    input.dataType,
                    file,
                    output.dataType,
                    s"${output.path}/$fileNameWithParentFolder",
                    basePath,
                    saveMode
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
              convert(
                input.dataType,
                subFolder,
                output.dataType,
                s"${output.path}/$subFolderName",
                basePath,
                saveMode
              ).flatMap(_ => {
                HdfsUtils.movePathContent(fs, subFolder, basePath)
              })
            })
      }
      _ = HdfsUtils.cleanup(fs, input.path)
    } yield res
  }

  /**
    * Handles data conversion for Local File System
    *
    * @param input The input DataHighway File Entity
    * @param output The output DataHighway File Entity
    * @param basePath The base path for input, output and processed folders
    * @param saveMode The file saving mode
    * @return List of List of Path as String, otherwise Throwable
    */
  private def handleLocalFS(
      input: models.File,
      output: models.File,
      basePath: String,
      saveMode: SaveMode
  ): Either[Throwable, List[List[String]]] = {
    for {
      folders <- FilesUtils.listNonEmptyFoldersRecursively(input.path)
      _ = logger.info("Folders to be processed : " + folders)
      filtered <- FilesUtils.filterNonEmptyFolders(folders)
      res <- input.dataType match {
        case XLSX =>
          FilesUtils
            .listFiles(filtered)
            .traverse(files => {
              files
                .traverse(file => {
                  val fileNameWithParentFolder =
                    FilesUtils.getFileNameAndParentFolderFromPath(file.toURI.getPath)
                  convert(
                    input.dataType,
                    file.toURI.getPath,
                    output.dataType,
                    s"${output.path}/$fileNameWithParentFolder",
                    basePath,
                    saveMode
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
            convert(
              input.dataType,
              subFolder,
              output.dataType,
              s"${output.path}/$subFolderName",
              basePath,
              saveMode
            ).flatMap(subInputFolder => {
              FilesUtils.movePathContent(subInputFolder, s"$basePath/processed")
            })
          })
      }
      _ = FilesUtils.cleanup(input.path)
    } yield res
  }
}
