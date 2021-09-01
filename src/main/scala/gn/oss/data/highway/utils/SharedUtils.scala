package gn.oss.data.highway.utils

import gn.oss.data.highway.models.{File, HDFS, Local, Output, Storage}

import java.util.UUID

object SharedUtils {

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
}
