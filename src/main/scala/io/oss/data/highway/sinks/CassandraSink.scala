package io.oss.data.highway.sinks

import io.oss.data.highway.models.{Cassandra, DataType, HDFS, Local, Storage}
import io.oss.data.highway.utils.{DataFrameUtils, FilesUtils, HdfsUtils}
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode
import cats.implicits._
import org.apache.hadoop.fs.FileSystem

import java.io.File

object CassandraSink extends HdfsUtils {

  val logger: Logger = Logger.getLogger(CassandraSink.getClass.getName)

  /**
    * Inserts csv file content into Cassandra
    *
    * @param inputPath The CSV input path
    * @param cassandra The Cassandra configs
    * @param saveMode The save mode
    * @param inputDataType The type of the input data
    * @return a Unit, otherwise a CassandraError
    */
  def insert(
      inputPath: String,
      cassandra: Cassandra,
      saveMode: SaveMode,
      inputDataType: DataType
  ): Either[Throwable, String] = {
    DataFrameUtils
      .loadDataFrame(inputPath, inputDataType)
      .map(df => {
        df.write
          .format("org.apache.spark.sql.cassandra")
          .option("keyspace", cassandra.keyspace)
          .option("table", cassandra.table)
          .mode(saveMode)
          .save()
        inputPath
      })
  }

  /**
    * Send data from input path to Cassandra
    *
    * @param in THe input path
    * @param cassandra The Cassandra Configs
    * @param saveMode The Spark save mode
    * @param storage The input file system storage
    * @param inputDataType The type of the input data
    * @return
    */
  def handleCassandraChannel(
      in: String,
      cassandra: Cassandra,
      saveMode: SaveMode,
      storage: Storage,
      inputDataType: DataType
  ): Either[Throwable, List[List[String]]] = {
    val basePath = new File(in).getParent
    storage match {
      case Local =>
        handleLocalFS(in, basePath, cassandra, saveMode, inputDataType)
      case HDFS =>
        handleHDFS(in, basePath, cassandra, saveMode, inputDataType, fs)
    }
  }

  /**
    * Handles data conversion for HDFS
    *
    * @param in The input data path
    * @param basePath The base path for input and output folders
    * @param cassandra The Cassandra Configs
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @param fs The provided File System
    * @return List of List of String, otherwise an Error
    */
  private def handleHDFS(
      in: String,
      basePath: String,
      cassandra: Cassandra,
      saveMode: SaveMode,
      inputDataType: DataType,
      fs: FileSystem
  ): Either[Throwable, List[List[String]]] = {
    for {
      folders <- HdfsUtils.listFolders(fs, in)
      _ = logger.info("folders : " + folders)
      filtered <- HdfsUtils.filterNonEmptyFolders(fs, folders)
      list <-
        filtered
          .traverse(subfolder => {
            insert(
              subfolder,
              cassandra,
              saveMode,
              inputDataType
            ).flatMap(_ => {
              HdfsUtils.movePathContent(fs, subfolder, basePath)
            })
          })
      _ = HdfsUtils.cleanup(fs, in)
    } yield list
  }

  /**
    * Handles data conversion for Local File System
    *
    * @param in The input data path
    * @param basePath The base path for input and output folders
    * @param cassandra The Cassandra Configs
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @return List of List of String, otherwise an Error
    */
  private def handleLocalFS(
      in: String,
      basePath: String,
      cassandra: Cassandra,
      saveMode: SaveMode,
      inputDataType: DataType
  ): Either[Throwable, List[List[String]]] = {
    for {
      folders <- FilesUtils.listNonEmptyFoldersRecursively(in)
      _ = logger.info("folders : " + folders)
      filtered <- FilesUtils.filterNonEmptyFolders(folders)
      list <-
        filtered
          .traverse(subFolder => {
            insert(
              subFolder,
              cassandra,
              saveMode,
              inputDataType
            ).flatMap(subInputFolder => {
              FilesUtils.movePathContent(subInputFolder, s"$basePath/processed")
            })
          })
      _ = FilesUtils.cleanup(in)
    } yield list
  }
}
