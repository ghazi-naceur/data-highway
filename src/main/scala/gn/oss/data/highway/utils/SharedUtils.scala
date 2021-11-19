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

  /**
    * Sets temporary file path that will be used for intermediate computations
    *
    * @param module The module name
    * @param storage The File System storage
    * @return a TemporaryLocation entity
    */
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

  /**
    * Sets temporary file path that will be used for intermediate computations in the Local File System
    *
    * @param module The module name
    * @return a TemporaryLocation entity
    */
  private def setLocalTempoFilePath(module: String): TemporaryLocation = {
    val tempoBasePath = s"${appConf.tmpWorkDir}/$module/${System.currentTimeMillis().toString}/"
    val temporaryPath = tempoBasePath + UUID.randomUUID().toString
    TemporaryLocation(temporaryPath, tempoBasePath)
  }

  /**
    * Sets the file system
    *
    * @param output The output entity
    * @param storage The file system storage
    * @return The file system storage
    */
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

  /**
    * Constructs the IO response
    *
    * @param input The input entity
    * @param output The output entity
    * @param dataHighwayResult The result of a Data Highway operation
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  def constructIOResponse(
    input: Input,
    output: Output,
    dataHighwayResult: Either[Throwable, Any]
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    dataHighwayResult match {
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
