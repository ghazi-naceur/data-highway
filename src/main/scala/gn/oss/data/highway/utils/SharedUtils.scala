package gn.oss.data.highway.utils

import gn.oss.data.highway.configs.{AppUtils, HdfsUtils}
import gn.oss.data.highway.models.DataHighwayError.DataHighwayFileError
import gn.oss.data.highway.models.{
  Cassandra,
  DataHighwayErrorResponse,
  DataHighwayIOResponse,
  DataHighwayResponse,
  Elasticsearch,
  File,
  HDFS,
  Input,
  Kafka,
  Local,
  Output,
  Plug,
  Postgres,
  Storage
}
import gn.oss.data.highway.utils.Constants.FAILURE

import java.util.UUID

object SharedUtils extends HdfsUtils with AppUtils {

  def setTempoFilePath(module: String, storage: Option[Storage]): (String, String) = {
    storage match {
      case Some(filesystem) =>
        filesystem match {
          case Local =>
            setLocalTempoFilePath(module)
          case HDFS =>
            val tuple = setLocalTempoFilePath(module)
            (
              s"${hadoopConf.host}:${hadoopConf.port}" + tuple._1,
              s"${hadoopConf.host}:${hadoopConf.port}" + tuple._2
            )
        }
      case None =>
        setLocalTempoFilePath(module)
    }
  }

  private def setLocalTempoFilePath(module: String): (String, String) = {
    val tempoBasePath =
      s"${appConf.tmpWorkDir}/$module/${System.currentTimeMillis().toString}/"
    val temporaryPath = tempoBasePath + UUID.randomUUID().toString
    (temporaryPath, tempoBasePath)
  }

  def setFileSystem(output: Output, storage: Option[Storage]): Storage = {
    storage match {
      case Some(fileSystem) =>
        output match {
          case File(_, _) =>
            fileSystem
          case _ =>
            Local
        }
      case None =>
        Local
    }
  }

  /**
    * Cleanups the temporary folder
    *
    * @param output The tmp base path
    * @param storage The tmp file system storage
    * @return Serializable
    */
  def cleanupTmp(output: String, storage: Option[Storage]): java.io.Serializable = {
    storage match {
      case Some(filesystem) =>
        filesystem match {
          case Local =>
            FilesUtils.delete(output)
          case HDFS =>
            HdfsUtils.delete(fs, output)
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

  def constructIOResponse(
      input: Input,
      output: Output,
      result: Either[Throwable, Any],
      message: String
  ): Either[DataHighwayErrorResponse, DataHighwayResponse] = {
    result match {
      case Right(_) =>
        Right(
          DataHighwayIOResponse(
            parsePlug(input),
            parsePlug(output),
            message
          )
        )
      case Left(thr) =>
        Left(
          DataHighwayErrorResponse(
            thr.getMessage,
            thr.getCause.toString,
            FAILURE
          )
        )
    }
  }

  private def parsePlug(plug: Plug): String = {
    plug match {
      case File(dataType, path)       => s"File: Path '$path' in '$dataType' format"
      case Cassandra(keyspace, table) => s"Cassandra: Keyspace '$keyspace' - Table '$table'"
      case Postgres(database, table)  => s"Postgres: Database '$database' - Table '$table'"
      case Elasticsearch(index, _, _) => s"Elasticsearch: Index '$index'"
      case Kafka(topic, _)            => s"Kafka: Topic '$topic'"
    }
  }
}
