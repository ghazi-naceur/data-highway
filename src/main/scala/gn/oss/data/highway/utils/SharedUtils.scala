package gn.oss.data.highway.utils

import com.typesafe.scalalogging.LazyLogging
import gn.oss.data.highway.configs.{AppUtils, HdfsUtils}
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
  Storage,
  TemporaryLocation
}
import gn.oss.data.highway.utils.Constants.EMPTY

import java.util.UUID

object SharedUtils extends HdfsUtils with AppUtils with LazyLogging {

  def setTempoFilePath(module: String, storage: Option[Storage]): TemporaryLocation = {
    storage match {
      case Some(filesystem) =>
        filesystem match {
          case Local => setLocalTempoFilePath(module)
          case HDFS =>
            val tuple = setLocalTempoFilePath(module)
            TemporaryLocation(
              s"${hadoopConf.host}:${hadoopConf.port}" + tuple.path,
              s"${hadoopConf.host}:${hadoopConf.port}" + tuple.basePath
            )
        }
      case None => setLocalTempoFilePath(module)
    }
  }

  private def setLocalTempoFilePath(module: String): TemporaryLocation = {
    val tempoBasePath = s"${appConf.tmpWorkDir}/$module/${System.currentTimeMillis().toString}/"
    val temporaryPath = tempoBasePath + UUID.randomUUID().toString
    TemporaryLocation(temporaryPath, tempoBasePath)
  }

  def setFileSystem(output: Output, storage: Option[Storage]): Storage = {
    storage match {
      case Some(fileSystem) =>
        output match {
          case File(_, _) => fileSystem
          case _          => Local
        }
      case None => Local
    }
  }

  def constructIOResponse(
    input: Input,
    output: Output,
    result: Either[Throwable, Any]
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    result match {
      case Right(_) =>
        logger.info(s"Successful route. Input: '$input' | Output: '$output'")
        Right(DataHighwaySuccess(Plug.summary(input), Plug.summary(output)))
      case Left(thr) =>
        logger.error(DataHighwayError.prettyError(thr))
        if (thr.getCause != null)
          Left(DataHighwayError(thr.getMessage, thr.getCause.toString))
        else
          Left(DataHighwayError(thr.getMessage, EMPTY))
    }
  }
}
