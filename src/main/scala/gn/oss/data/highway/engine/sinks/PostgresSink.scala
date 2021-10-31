package gn.oss.data.highway.engine.sinks

import gn.oss.data.highway.configs.HdfsUtils
import gn.oss.data.highway.models
import gn.oss.data.highway.utils.Constants.EMPTY
import gn.oss.data.highway.utils.{DataFrameUtils, FilesUtils, HdfsUtils, SharedUtils}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode

import java.io.File
import cats.implicits._
import gn.oss.data.highway.models.DataHighwayRuntimeException.MustHaveFileSystemAndSaveModeError
import gn.oss.data.highway.models.{
  Consistency,
  DataHighwayErrorResponse,
  DataHighwaySuccessResponse,
  DataType,
  HDFS,
  Local,
  Postgres,
  PostgresDB,
  Storage,
  XLSX
}

object PostgresSink extends HdfsUtils {

  val logger: Logger = Logger.getLogger(PostgresSink.getClass.getName)

  /**
    * Inserts file content into Postgres
    *
    * @param inputDataType The input data type path
    * @param inputPath The input path
    * @param output The output Postgres Entity
    * @param saveMode The file saving mode
    * @return Path as String, otherwise an Throwable
    */
  def insert(
      inputDataType: DataType,
      inputPath: String,
      output: Postgres,
      saveMode: SaveMode
  ): Either[Throwable, String] = {
    // todo maybe fuse input, output and datatype
    val result = for {
      dataframe <- DataFrameUtils.loadDataFrame(inputDataType, inputPath)
      _ <- DataFrameUtils.saveDataFrame(
        dataframe,
        PostgresDB(output.database, output.table),
        EMPTY,
        saveMode
      )
    } yield ()
    result match {
      case Right(_)  => Right(inputPath)
      case Left(thr) => Left(thr)
    }
  }

  /**
    * Inserts files content into Postgres
    *
    * @param input The input DataHighway File Entity
    * @param output The output DataHighway File Entity
    * @param storage The file system storage : It can be Local or HDFS
    * @param consistency The file saving mode
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  def handlePostgresChannel(
      input: models.File,
      output: Postgres,
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
    * Handles inserting data from Local File System to Postgres
    *
    * @param input The input DataHighway File Entity
    * @param output The output Postgres Entity
    * @param basePath The base path for input, output and processed folders
    * @param saveMode The file saving mode
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  private def handleLocalFS(
      input: models.File,
      output: Postgres,
      basePath: String,
      saveMode: SaveMode
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    // todo maybe substitute for/yield
    val result = for {
      res <- insertRows(input, output, basePath, saveMode)
      _ = FilesUtils.cleanup(input.path)
    } yield res
    SharedUtils.constructIOResponse(input, output, result)
  }

  /**
    * Handles inserting data from HDFS to Postgres
    *
    * @param input The input DataHighway File Entity
    * @param output The output Postgres Entity
    * @param basePath The base path for input, output and processed folders
    * @param saveMode The file saving mode
    * @param fs The provided File System
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  private def handleHDFS(
      input: models.File,
      output: Postgres,
      basePath: String,
      saveMode: SaveMode,
      fs: FileSystem
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    val result = for {
      folders <- HdfsUtils.listFolders(fs, input.path)
      _ = logger.info("Folders to be processed : " + folders)
      filtered <- HdfsUtils.filterNonEmptyFolders(fs, folders)
      res <- input.dataType match {
        case XLSX =>
          filtered
            .traverse(subfolder => {
              HdfsUtils
                .listFiles(fs, subfolder)
                .traverse(file => insert(input.dataType, file, output, saveMode))
                .flatMap(_ => HdfsUtils.movePathContent(fs, subfolder, basePath))
            })
        case _ =>
          filtered
            .traverse(subfolder =>
              insert(input.dataType, subfolder, output, saveMode)
                .flatMap(_ => HdfsUtils.movePathContent(fs, subfolder, basePath))
            )
      }
      _ = HdfsUtils.cleanup(fs, input.path)
    } yield res
    SharedUtils.constructIOResponse(input, output, result)
  }

  /**
    * Inserts rows in Postgres
    *
    * @param input The File input entity
    * @param output The Postgres output entity
    * @param basePath The File entity base path
    * @param saveMode The Spark save mode
    * @return List of List of Path as String, otherwise a Throwable
    */
  def insertRows(
      input: models.File,
      output: Postgres,
      basePath: String,
      saveMode: SaveMode
  ): Either[Throwable, List[List[String]]] = {
    // todo to be refined
    for {
      folders <- FilesUtils.listNonEmptyFoldersRecursively(input.path)
      _ = logger.info("Folders to be processed : " + folders)
      filtered <- FilesUtils.filterNonEmptyFolders(folders)
      res <- input.dataType match {
        case XLSX =>
          FilesUtils
            .listFiles(filtered)
            .traverse(files => {
              files.traverse(file => {
                insert(input.dataType, file.toURI.getPath, output, saveMode)
                  .flatMap(subInputFolder =>
                    FilesUtils
                      .movePathContent(
                        subInputFolder,
                        s"$basePath/processed/${new File(
                          subInputFolder
                        ).getParentFile.toURI.getPath.split("/").takeRight(1).mkString("/")}"
                      )
                  )
              })
            })
            .flatten
        case _ =>
          filtered
            .traverse(subFolder => {
              insert(input.dataType, subFolder, output, saveMode)
                .flatMap(subInputFolder =>
                  FilesUtils.movePathContent(subInputFolder, s"$basePath/processed")
                )
            })
      }
    } yield res
  }
}
