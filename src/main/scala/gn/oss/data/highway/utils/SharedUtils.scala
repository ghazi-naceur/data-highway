package gn.oss.data.highway.utils

import gn.oss.data.highway.models.DataHighwayError.DataHighwayFileError
import gn.oss.data.highway.models.{File, HDFS, Local, Output, Storage}

import java.util.UUID

object SharedUtils extends HdfsUtils {

  def setTempoFilePath(module: String, storage: Option[Storage]): (String, String) = {
    storage match {
      case Some(filesystem) =>
        filesystem match {
          case Local =>
            setLocalTempoFilePath(module)
          case HDFS =>
            val tuple = setLocalTempoFilePath(module)
            (HdfsUtils.hadoopConf.host + tuple._1, HdfsUtils.hadoopConf.host + tuple._2)
        }
      case None =>
        setLocalTempoFilePath(module)
    }
  }

  private def setLocalTempoFilePath(module: String): (String, String) = {
    val tempoBasePath =
      s"/tmp/data-highway/$module/${System.currentTimeMillis().toString}/"
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
}
