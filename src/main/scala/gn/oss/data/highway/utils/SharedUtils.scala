package gn.oss.data.highway.utils

import gn.oss.data.highway.configs.{AppUtils, HdfsUtils}
import gn.oss.data.highway.models.DataHighwayErrorObj.DataHighwayFileError
import gn.oss.data.highway.models.{
  DataHighwayError,
  DataHighwayErrorResponse,
  DataHighwaySuccess,
  DataHighwaySuccessResponse,
  File,
  HDFS,
  Input,
  Local,
  Output,
  Plug,
  Storage
}

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
      result: Either[Throwable, Any]
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    result match {
      case Right(_) =>
        Right(
          DataHighwaySuccess(
            Plug.summary(input),
            Plug.summary(output)
          )
        )
      case Left(thr) =>
        Left(
          DataHighwayError(
            thr.getMessage,
            thr.getCause.toString
          )
        )
    }
  }
}
