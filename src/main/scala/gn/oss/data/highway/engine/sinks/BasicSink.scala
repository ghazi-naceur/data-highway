package gn.oss.data.highway.engine.sinks

import gn.oss.data.highway.models
import gn.oss.data.highway.configs.HdfsUtils
import gn.oss.data.highway.utils.{DataFrameUtils, FilesUtils, HdfsUtils, SharedUtils}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode
import cats.implicits._
import gn.oss.data.highway.models.DataHighwayRuntimeException.MustHaveFileSystemAndSaveModeError
import gn.oss.data.highway.models.{
  Consistency,
  DataHighwayErrorResponse,
  DataHighwaySuccessResponse,
  DataType,
  HDFS,
  Local,
  Storage,
  XLSX
}

import java.io.File

object BasicSink extends HdfsUtils {

  val logger: Logger = Logger.getLogger(BasicSink.getClass.getName)

  /**
    * Converts file
    *
    * @param inputDataType The input data type path
    * @param inputPath The input path
    * @param outputDataType The input data type path
    * @param outputPath The input path
    * @param saveMode The file saving mode
    * @return Path as String, otherwise an Throwable
    */
  def convert(
      inputDataType: DataType,
      inputPath: String,
      outputDataType: DataType,
      outputPath: String,
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
    * @param consistency The file saving mode
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  def handleChannel(
      input: models.File,
      output: models.File,
      storage: Option[Storage],
      consistency: Option[Consistency]
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    val basePath = new File(input.path).getParent
    (storage, consistency) match {
      case (Some(filesystem), Some(consist)) =>
        filesystem match {
          case Local => handleLocalFS(input, output, basePath, consist.toSaveMode)
          case HDFS  => handleHDFS(input, output, basePath, consist.toSaveMode, fs)
        }
      case (_, _) => Left(MustHaveFileSystemAndSaveModeError)
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
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  private def handleHDFS(
      input: models.File,
      output: models.File,
      basePath: String,
      saveMode: SaveMode,
      fs: FileSystem
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    // todo to be refined
    val result = for {
      folders <- HdfsUtils.listFolders(fs, input.path)
      _ = logger.info("Folders to be processed : " + folders)
      filtered <- HdfsUtils.filterNonEmptyFolders(fs, folders)
      res <- input.dataType match {
        case XLSX =>
          filtered
            .traverse(subFolder => {
              HdfsUtils
                .listFiles(fs, subFolder)
                .traverse(file => {
                  val fullOutputPath =
                    s"${output.path}/${FilesUtils.getFileNameAndParentFolderFromPath(file)}"
                  convert(input.dataType, file, output.dataType, fullOutputPath, saveMode)
                })
                .flatMap(_ => HdfsUtils.movePathContent(fs, subFolder, basePath))
            })
        case _ =>
          filtered
            .traverse(subFolder => {
              val fullOutputPath = s"${output.path}/${subFolder.split("/").last}"
              convert(input.dataType, subFolder, output.dataType, fullOutputPath, saveMode)
                .flatMap(_ => HdfsUtils.movePathContent(fs, subFolder, basePath))
            })
      }
      _ = HdfsUtils.cleanup(fs, input.path)
    } yield res
    SharedUtils.constructIOResponse(input, output, result)
  }

  /**
    * Handles data conversion for Local File System
    *
    * @param input The input DataHighway File Entity
    * @param output The output DataHighway File Entity
    * @param basePath The base path for input, output and processed folders
    * @param saveMode The file saving mode
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  private def handleLocalFS(
      input: models.File,
      output: models.File,
      basePath: String,
      saveMode: SaveMode
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    // todo to be refined
    val result = for {
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
                  val fullOutputPath =
                    s"${output.path}/${FilesUtils.getFileNameAndParentFolderFromPath(file.toURI.getPath)}"
                  convert(
                    input.dataType,
                    file.toURI.getPath,
                    output.dataType,
                    fullOutputPath,
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
            val fullOutPutPath =
              s"${output.path}/${FilesUtils.reversePathSeparator(subFolder).split("/").last}"
            convert(input.dataType, subFolder, output.dataType, fullOutPutPath, saveMode)
              .flatMap(subInputFolder =>
                FilesUtils.movePathContent(subInputFolder, s"$basePath/processed")
              )
          })
      }
      _ = FilesUtils.cleanup(input.path)
    } yield res
    SharedUtils.constructIOResponse(input, output, result)
  }
}
