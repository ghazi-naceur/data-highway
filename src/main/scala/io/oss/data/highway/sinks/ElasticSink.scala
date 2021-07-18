package io.oss.data.highway.sinks

import io.oss.data.highway.models.{FileSystem, HDFS, JSON, Local}
import io.oss.data.highway.utils.{ElasticUtils, FilesUtils, HdfsUtils}
import org.apache.log4j.Logger
import cats.implicits._
import com.sksamuel.elastic4s.requests.common.RefreshPolicy

import java.io.File

object ElasticSink extends ElasticUtils {

  val logger: Logger = Logger.getLogger(ElasticSink.getClass.getName)

  /**
    * Indexes file's content into Elasticsearch
    *
    * @param in The input data path
    * @param out The Elasticsearch index
    * @param basePath The base path for input, output and processed folders
    * @param fileSystem The input file system
    * @param bulkEnabled The flag to enable/disable the elastic Bulk
    * @return a Unit, otherwise an Error
    */
  def sendToElasticsearch(
      in: String,
      out: String,
      basePath: String,
      fileSystem: FileSystem,
      bulkEnabled: Boolean
  ): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      if (!bulkEnabled)
        indexWithIndexQuery(in, out, basePath, fileSystem)
      else
        indexWithBulkQuery(in, out, basePath, fileSystem)
      logger.info(s"Successfully indexing data from '$in' into the '$out' index.")
    }
  }

  /**
    * Indexes data using an ES IndexQuery
    *
    * @param in The input data folder
    * @param out The ES index
    * @param basePath The base path of the input folder
    * @param fileSystem The input file system : Local or HDFS
    * @return Any
    */
  private def indexWithIndexQuery(
      in: String,
      out: String,
      basePath: String,
      fileSystem: FileSystem
  ): Any = {
    fileSystem match {
      case HDFS =>
        HdfsUtils
          .listFilesRecursively(in)
          .foreach(file => {
            HdfsUtils
              .getJsonLines(file)
              .foreach(line => indexDocInEs(out, line))
            HdfsUtils.movePathContent(file, basePath)
          })
      case Local =>
        FilesUtils
          .listFilesRecursively(new File(in), Seq(JSON.extension))
          .foreach(file => {
            FilesUtils
              .getJsonLines(file.getAbsolutePath)
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
    * @param fileSystem The input file system : Local or HDFS
    * @return Any
    */
  private def indexWithBulkQuery(
      in: String,
      out: String,
      basePath: String,
      fileSystem: FileSystem
  ): Any = {
    import com.sksamuel.elastic4s.ElasticDsl._

    fileSystem match {
      case HDFS =>
        HdfsUtils
          .listFilesRecursively(in)
          .foreach(file => {
            val queries = HdfsUtils
              .getJsonLines(file)
              .map(line => {
                indexInto(out) doc line refresh RefreshPolicy.IMMEDIATE
              })
            esClient.execute {
              bulk(queries).refresh(RefreshPolicy.Immediate)
            }.await
            HdfsUtils.movePathContent(file, basePath)
          })

      case Local =>
        FilesUtils
          .listFilesRecursively(new File(in), Seq(JSON.extension))
          .foreach(file => {
            val queries = FilesUtils
              .getJsonLines(file.getAbsolutePath)
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
    * @param fileSystem The input file system
    * @param bulkEnabled A flag to specify if the Elastic Bulk is enabled or not
    * @return List of Unit, otherwise an Throwable
    */
  def handleElasticsearchChannel(
      in: String,
      out: String,
      fileSystem: FileSystem,
      bulkEnabled: Boolean
  ): Either[Throwable, List[Unit]] = {
    val basePath = new File(in).getParent

    fileSystem match {
      case HDFS =>
        for {
          list <-
            HdfsUtils
              .listFolders(in)
              .traverse(folders => {
                folders.traverse(folder => {
                  sendToElasticsearch(folder, out, basePath, fileSystem, bulkEnabled)
                })
              })
              .flatten
          _ = HdfsUtils.cleanup(in)
        } yield list
      case Local =>
        for {
          folders <- FilesUtils.listFoldersRecursively(in)
          list <-
            folders
              .filterNot(path => new File(path).listFiles.filter(_.isFile).toList.isEmpty)
              .traverse(folder => {
                sendToElasticsearch(folder, out, basePath, fileSystem, bulkEnabled)
              })
          _ = FilesUtils.cleanup(in)
        } yield list
    }
  }
}
