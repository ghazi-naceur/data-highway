package io.oss.data.highway.engine

import io.oss.data.highway.models.{HDFS, JSON, Local, Storage}
import io.oss.data.highway.utils.{ElasticUtils, FilesUtils, HdfsUtils}
import org.apache.log4j.Logger
import cats.implicits._
import com.sksamuel.elastic4s.requests.common.RefreshPolicy

import java.io.File

object ElasticSink extends ElasticUtils with HdfsUtils {

  val logger: Logger = Logger.getLogger(ElasticSink.getClass.getName)

  /**
    * Indexes file's content into Elasticsearch
    *
    * @param in The input data path
    * @param out The Elasticsearch index
    * @param basePath The base path for input, output and processed folders
    * @param storage The input file system storage
    * @param bulkEnabled The flag to enable/disable the elastic Bulk
    * @return a Unit, otherwise an Error
    */
  def sendToElasticsearch(
      in: String,
      out: String,
      basePath: String,
      storage: Storage,
      bulkEnabled: Boolean
  ): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      if (!bulkEnabled)
        indexWithIndexQuery(in, out, basePath, storage)
      else
        indexWithBulkQuery(in, out, basePath, storage)
      logger.info(s"Successfully indexing data from '$in' into the '$out' index.")
    }
  }

  /**
    * Indexes data using an ES IndexQuery
    *
    * @param in The input data folder
    * @param out The ES index
    * @param basePath The base path of the input folder
    * @param storage The input file system storage : Local or HDFS
    * @return Any
    */
  private def indexWithIndexQuery(
      in: String,
      out: String,
      basePath: String,
      storage: Storage
  ): Any = {
    storage match {
      case HDFS =>
        HdfsUtils
          .listFilesRecursively(fs, in)
          .foreach(file => {
            HdfsUtils
              .getLines(fs, file)
              .foreach(line => indexDocInEs(out, line))
            HdfsUtils.movePathContent(fs, file, basePath)
          })
      case Local =>
        FilesUtils
          .listFilesRecursively(new File(in), JSON.extension)
          .foreach(file => {
            FilesUtils
              .getLines(file.getAbsolutePath)
              .foreach(line => indexDocInEs(out, line))
            FilesUtils
              .movePathContent(
                file.getAbsolutePath,
                s"$basePath/processed/${file.getParentFile.getName}"
              )
          })
    }
  }

  /**
    * Indexes data using an ES BulkQuery
    *
    * @param in The input data folder
    * @param out The ES index
    * @param basePath The base path of the input folder
    * @param storage The input file system storage : Local or HDFS
    * @return Any
    */
  private def indexWithBulkQuery(
      in: String,
      out: String,
      basePath: String,
      storage: Storage
  ): Any = {
    import com.sksamuel.elastic4s.ElasticDsl._

    storage match {
      case HDFS =>
        HdfsUtils
          .listFilesRecursively(fs, in)
          .foreach(file => {
            val queries = HdfsUtils
              .getLines(fs, file)
              .map(line => {
                indexInto(out) doc line refresh RefreshPolicy.IMMEDIATE
              })
            esClient.execute {
              bulk(queries).refresh(RefreshPolicy.Immediate)
            }.await
            HdfsUtils.movePathContent(fs, file, basePath)
          })

      case Local =>
        FilesUtils
          .listFilesRecursively(new File(in), JSON.extension)
          .foreach(file => {
            val queries = FilesUtils
              .getLines(file.getAbsolutePath)
              .map(line => {
                indexInto(out) doc line refresh RefreshPolicy.IMMEDIATE
              })
              .toSeq
            esClient.execute {
              bulk(queries).refresh(RefreshPolicy.Immediate)
            }.await
            FilesUtils.movePathContent(
              file.getAbsolutePath,
              s"$basePath/processed/${file.getParentFile.getName}"
            )
          })
    }
  }

  /**
    * Indexes document in Elasticsearch
    *
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
    * @param storage The input file system storage
    * @param bulkEnabled A flag to specify if the Elastic Bulk is enabled or not
    * @return List of Unit, otherwise an Throwable
    */
  def handleElasticsearchChannel(
      in: String,
      out: String,
      storage: Storage,
      bulkEnabled: Boolean
  ): Either[Throwable, List[Unit]] = {
    val basePath = new File(in).getParent

    storage match {
      case HDFS =>
        for {
          list <-
            HdfsUtils
              .listFolders(fs, in)
              .traverse(folders => {
                folders.traverse(folder => {
                  sendToElasticsearch(folder, out, basePath, storage, bulkEnabled)
                })
              })
              .flatten
          _ = HdfsUtils.cleanup(fs, in)
        } yield list
      case Local =>
        for {
          folders <- FilesUtils.listNonEmptyFoldersRecursively(in)
          list <-
            folders
              .filterNot(path => new File(path).listFiles.filter(_.isFile).toList.isEmpty)
              .traverse(folder => {
                sendToElasticsearch(folder, out, basePath, storage, bulkEnabled)
              })
          _ = FilesUtils.cleanup(in)
        } yield list
    }
  }
}