package io.oss.data.highway.converter

import io.oss.data.highway.model.{ElasticConfig, JSON}
import io.oss.data.highway.utils.{ElasticUtils, FilesUtils}
import org.apache.log4j.Logger
import cats.implicits._
import com.sksamuel.elastic4s.requests.common.RefreshPolicy

import java.io.File
import java.nio.file.Path

object ElasticSink extends ElasticUtils {

  val logger: Logger = Logger.getLogger(ElasticSink.getClass.getName)

  /**
    * Send file to Elasticsearch
    *
    * @param in The input data path
    * @param out The Elasticsearch index
    * @param basePath The base path for input, output and processed folders
    * @return a List of Path, otherwise an Error
    */
  def sendToElasticsearch(
      in: String,
      out: String,
      basePath: String): Either[Throwable, List[Path]] = {

    if (new File(in).isFile) {
      for (line <- FilesUtils.getJsonLines(in)) {
        indexDocInEs(out, line)
        val suffix = new File(in).getParent.split("/").last
        FilesUtils.movePathContent(in, basePath, s"processed/$suffix")
      }
    } else {
      FilesUtils
        .listFilesRecursively(new File(in), Seq(JSON.extension))
        .foreach(file => {
          for (line <- FilesUtils.getJsonLines(file.getAbsolutePath)) {
            indexDocInEs(out, line)
            val suffix = new File(file.getAbsolutePath).getParent.split("/").last
            FilesUtils.movePathContent(file.getAbsolutePath, basePath, s"processed/$suffix")
          }
        })
    }

    logger.info(s"Successfully indexing data from '$in' into the '$out' index.")
    FilesUtils.movePathContent(in, basePath)
  }

  /**
    * Indexes document in Elasticsearch
    * @param out The Elasticsearch index
    * @param line The document to be sent to Elasticsearch
    */
  private def indexDocInEs(out: String, line: String): Unit = {
    import com.sksamuel.elastic4s.ElasticDsl._
    esClient.execute {
      indexInto(out) doc line refresh RefreshPolicy.IMMEDIATE
    }.await
    logger.info(s"Index: '$out' - Sent data: '$line'")
  }

  /**
    * Send files to Elasticsearch
    *
    * @param in The input data path
    * @param out The elasticsearch index
    * @return List of List of Path, otherwise an Error
    */
  def handleElasticsearchChannel(
      in: String,
      out: String): Either[Throwable, List[List[Path]]] = {
    val basePath = new File(in).getParent
    for {
      folders <- FilesUtils.listFoldersRecursively(in)
      list <- folders
        .filterNot(path =>
          new File(path).listFiles.filter(_.isFile).toList.isEmpty)
        .traverse(folder => {
          sendToElasticsearch(folder, out, basePath)
        })
      _ = FilesUtils.deleteFolder(in)
    } yield list
  }
}
