package gn.oss.data.highway.engine.sinks

import gn.oss.data.highway.configs.HdfsUtils
import gn.oss.data.highway.models
import gn.oss.data.highway.utils.Constants.EMPTY
import gn.oss.data.highway.utils.{DataFrameUtils, FilesUtils, HdfsUtils, SharedUtils}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SaveMode
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import gn.oss.data.highway.models.DataHighwayRuntimeException.MustHaveFileSystemAndSaveModeError
import gn.oss.data.highway.models.{
  Cassandra,
  CassandraDB,
  Consistency,
  DBConnector,
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

import java.io.File

object DBConnectorSink extends HdfsUtils with LazyLogging {

  /**
    * Inserts file content into Cassandra
    *
    * @param inputDataType The input data type path
    * @param inputPath The input path
    * @param output The output DBConnector Entity: Cassandra or Postgres
    * @param saveMode The file saving mode
    * @return Path as String, otherwise an Throwable
    */
  def insert(
    inputDataType: DataType,
    inputPath: String,
    output: DBConnector,
    saveMode: SaveMode
  ): Either[Throwable, String] = {
    for {
      dataframe <- DataFrameUtils.loadDataFrame(inputDataType, inputPath)
      _ <- output match {
        case Cassandra(keyspace, table) =>
          DataFrameUtils.saveDataFrame(dataframe, CassandraDB(keyspace, table), EMPTY, saveMode)
        case Postgres(database, table) =>
          DataFrameUtils.saveDataFrame(dataframe, PostgresDB(database, table), EMPTY, saveMode)
      }
    } yield inputPath
  }

  /**
    * Handles file-to-dbconnector route. The output can be Cassandra or Postgres.
    *
    * @param input The input DataHighway File Entity
    * @param output The output DBConnector Entity: Cassandra or Postgres
    * @param storage The file system storage : It can be Local or HDFS
    * @param consistency The file saving mode
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  def handleDBConnectorChannel(
    input: models.File,
    output: DBConnector,
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
    * Handles inserting data from HDFS to Cassandra
    *
    * @param input The input DataHighway File Entity
    * @param output The output DBConnector Entity: Cassandra or Postgres
    * @param basePath The base path for input, output and processed folders
    * @param saveMode The file saving mode
    * @param fs The provided File System
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  private def handleHDFS(
    input: models.File,
    output: DBConnector,
    basePath: String,
    saveMode: SaveMode,
    fs: FileSystem
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    val result = for {
      folders <- HdfsUtils.listFolders(fs, input.path)
      _ = logger.info("Folders to be processed : " + folders)
      filtered <- HdfsUtils.filterNonEmptyFolders(fs, folders)
      result <- input.dataType match {
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
            .traverse(subfolder => {
              insert(input.dataType, subfolder, output, saveMode)
                .flatMap(_ => HdfsUtils.movePathContent(fs, subfolder, basePath))
            })
      }
      _ = HdfsUtils.cleanup(fs, input.path)
    } yield result
    SharedUtils.constructIOResponse(input, output, result)
  }

  /**
    * Handles inserting data from Local File System to Cassandra
    *
    * @param input The input DataHighway File Entity
    * @param output The output DBConnector Entity: Cassandra or Postgres
    * @param basePath The base path for input, output and processed folders
    * @param saveMode The file saving mode
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  private def handleLocalFS(
    input: models.File,
    output: DBConnector,
    basePath: String,
    saveMode: SaveMode
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    val result = insertRows(input, output, basePath, saveMode)
    FilesUtils.cleanup(input.path)
    SharedUtils.constructIOResponse(input, output, result)
  }

  /**
    * Inserts rows in Cassandra
    *
    * @param input The File input entity
    * @param output The DBConnector output entity: Cassandra or Postgres
    * @param basePath The File entity base path
    * @param saveMode The Spark save mode
    * @return List of List of Path as String, otherwise a Throwable
    */
  def insertRows(
    input: models.File,
    output: DBConnector,
    basePath: String,
    saveMode: SaveMode
  ): Either[Throwable, List[List[String]]] = {
    for {
      folders <- FilesUtils.listNonEmptyFoldersRecursively(input.path)
      _ = logger.info("Folders to be processed : " + folders)
      filtered <- FilesUtils.filterNonEmptyFolders(folders)
      result <- input.dataType match {
        case XLSX =>
          for {
            files <- FilesUtils.listFiles(filtered)
            processedFolders <- files.traverse(file => {
              insert(input.dataType, file.toURI.getPath, output, saveMode)
                .flatMap(subInputFolder => {
                  val baseFolderName =
                    s"${new File(subInputFolder).getParentFile.toURI.getPath.split("/").takeRight(1).mkString("/")}"
                  FilesUtils.movePathContent(subInputFolder, s"$basePath/processed/$baseFolderName")
                })
            })
          } yield processedFolders
        case _ =>
          filtered
            .traverse(subFolder => {
              insert(input.dataType, subFolder, output, saveMode)
                .flatMap(subInputFolder => FilesUtils.movePathContent(subInputFolder, s"$basePath/processed"))
            })
      }
    } yield result
  }
}
