package gn.oss.data.highway.engine

import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode
import cats.implicits._
import gn.oss.data.highway.models
import gn.oss.data.highway.models.{
  Cassandra,
  CassandraDB,
  DataHighwayErrorResponse,
  DataHighwayResponse,
  DataType,
  HDFS,
  Local,
  Storage,
  XLSX
}
import gn.oss.data.highway.utils.{DataFrameUtils, FilesUtils, HdfsUtils, SharedUtils}
import gn.oss.data.highway.utils.Constants.{EMPTY, SUCCESS}
import org.apache.hadoop.fs.FileSystem

import java.io.File

object CassandraSink extends HdfsUtils {

  val logger: Logger = Logger.getLogger(CassandraSink.getClass.getName)

  /**
    * Inserts file content into Cassandra
    *
    * @param inputDataType The input data type path
    * @param inputPath The input path
    * @param output The output Cassandra Entity
    * @param saveMode The file saving mode
    * @return Path as String, otherwise an Throwable
    */
  def insert(
      inputDataType: DataType,
      inputPath: String,
      output: Cassandra,
      saveMode: SaveMode
  ): Either[Throwable, String] = {
    DataFrameUtils
      .loadDataFrame(inputDataType, inputPath)
      .map(df => {
        DataFrameUtils
          .saveDataFrame(df, CassandraDB(output.keyspace, output.table), EMPTY, saveMode)
        inputPath
      })
  }

  /**
    * Inserts files content into Cassandra
    *
    * @param input The input DataHighway File Entity
    * @param output The output DataHighway File Entity
    * @param storage The file system storage : It can be Local or HDFS
    * @param saveMode The file saving mode
    * @return DataHighwayFileResponse, otherwise a DataHighwayErrorResponse
    */
  def handleCassandraChannel(
      input: models.File,
      output: Cassandra,
      storage: Option[Storage],
      saveMode: SaveMode
  ): Either[DataHighwayErrorResponse, DataHighwayResponse] = {
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
          DataHighwayErrorResponse(
            "MissingFileSystemStorage",
            "Missing 'storage' field",
            ""
          )
        )
    }
  }

  /**
    * Handles inserting data from HDFS to Cassandra
    *
    * @param input The input DataHighway File Entity
    * @param output The output Cassandra Entity
    * @param basePath The base path for input, output and processed folders
    * @param saveMode The file saving mode
    * @param fs The provided File System
    * @return DataHighwayFileResponse, otherwise a DataHighwayErrorResponse
    */
  private def handleHDFS(
      input: models.File,
      output: Cassandra,
      basePath: String,
      saveMode: SaveMode,
      fs: FileSystem
  ): Either[DataHighwayErrorResponse, DataHighwayResponse] = {
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
                .traverse(file => {
                  insert(
                    input.dataType,
                    file,
                    output,
                    saveMode
                  )
                })
                .flatMap(_ => {
                  HdfsUtils.movePathContent(fs, subfolder, basePath)
                })
            })
        case _ =>
          filtered
            .traverse(subfolder => {
              insert(
                input.dataType,
                subfolder,
                output,
                saveMode
              ).flatMap(_ => {
                HdfsUtils.movePathContent(fs, subfolder, basePath)
              })
            })
      }
      _ = HdfsUtils.cleanup(fs, input.path)
    } yield res
    SharedUtils.constructIOResponse(input, output, result, SUCCESS)
  }

  /**
    * Handles inserting data from Local File System to Cassandra
    *
    * @param input The input DataHighway File Entity
    * @param output The output Cassandra Entity
    * @param basePath The base path for input, output and processed folders
    * @param saveMode The file saving mode
    * @return DataHighwayFileResponse, otherwise a DataHighwayErrorResponse
    */
  private def handleLocalFS(
      input: models.File,
      output: Cassandra,
      basePath: String,
      saveMode: SaveMode
  ): Either[DataHighwayErrorResponse, DataHighwayResponse] = {
    val result = for {
      res <- insertRows(input, output, basePath, saveMode)
      _ = FilesUtils.cleanup(input.path)
    } yield res
    SharedUtils.constructIOResponse(input, output, result, SUCCESS)
  }

  /**
    * Inserts rows in Cassandra
    *
    * @param input The File input entity
    * @param output The Cassandra output entity
    * @param basePath The File entity base path
    * @param saveMode The Spark save mode
    * @return List of List of Path as String, otherwise a Throwable
    */
  def insertRows(
      input: models.File,
      output: Cassandra,
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
              files.traverse(file => {
                insert(
                  input.dataType,
                  file.toURI.getPath,
                  output,
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
          filtered
            .traverse(subFolder => {
              insert(
                input.dataType,
                subFolder,
                output,
                saveMode
              ).flatMap(subInputFolder => {
                FilesUtils.movePathContent(subInputFolder, s"$basePath/processed")
              })
            })
      }
    } yield res
  }
}
