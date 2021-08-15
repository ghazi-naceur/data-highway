package io.oss.data.highway.engine

import io.oss.data.highway.models.{Elasticsearch, HDFS, JSON, Local, Storage}
import io.oss.data.highway.utils.{ElasticUtils, FilesUtils, HdfsUtils}
import org.apache.log4j.Logger
import cats.implicits._
import com.sksamuel.elastic4s.requests.common.RefreshPolicy
import io.oss.data.highway.models
import io.oss.data.highway.models.DataHighwayError.DataHighwayFileError

import java.io.File

object ElasticSink extends ElasticUtils with HdfsUtils {

  val logger: Logger = Logger.getLogger(ElasticSink.getClass.getName)

  /**
    * Indexes file's content into Elasticsearch
    *
    * @param input The input data path
    * @param output The output Elasticsearch entity
    * @param basePath The base path for input, output and processed folders
    * @param storage The input file system storage
    * @return a Unit, otherwise an Error
    */
  private def sendToElasticsearch(
      input: String,
      output: Elasticsearch,
      basePath: String,
      storage: Storage
  ): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      if (!output.bulkEnabled)
        indexWithIndexQuery(input, output.index, basePath, storage)
      else
        indexWithBulkQuery(input, output.index, basePath, storage)
      logger.info(s"Successfully indexing data from '$input' into the '$output' index.")
    }
  }

  /**
    * Indexes data using an ES IndexQuery
    *
    * @param input The input data folder
    * @param output The output Elasticsearch entity
    * @param basePath The base path of the input folder
    * @param storage The input file system storage : Local or HDFS
    * @return Any
    */
  private def indexWithIndexQuery(
      input: String,
      output: String,
      basePath: String,
      storage: Storage
  ): Any = {
    storage match {
      case HDFS =>
        HdfsUtils
          .listFilesRecursively(fs, input)
          .foreach(file => {
            HdfsUtils
              .getLines(fs, file)
              .foreach(line => indexDocInEs(output, line))
            HdfsUtils.movePathContent(fs, file, basePath)
          })
      case Local =>
        FilesUtils
          .listFilesRecursively(new File(input), JSON.extension)
          .foreach(file => {
            FilesUtils
              .getLines(file.getAbsolutePath)
              .foreach(line => indexDocInEs(output, line))
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
    * Indexes files to Elasticsearch
    *
    * @param input The input File entity
    * @param output The output Elasticsearch entity
    * @param storage The input file system storage
    * @return List of Unit, otherwise an Throwable
    */
  def handleElasticsearchChannel(
      input: models.File,
      output: Elasticsearch,
      storage: Option[Storage]
  ): Either[Throwable, List[Unit]] = {
    val basePath = new File(input.path).getParent

    storage match {
      case Some(value) =>
        value match {
          case HDFS =>
            for {
              list <-
                HdfsUtils
                  .listFolders(fs, input.path)
                  .traverse(folders => {
                    folders.traverse(folder => {
                      sendToElasticsearch(folder, output, basePath, value)
                    })
                  })
                  .flatten
              _ = HdfsUtils.cleanup(fs, input.path)
            } yield list
          case Local =>
            for {
              folders <- FilesUtils.listNonEmptyFoldersRecursively(input.path)
              list <-
                folders
                  .filterNot(path => new File(path).listFiles.filter(_.isFile).toList.isEmpty)
                  .traverse(folder => {
                    sendToElasticsearch(folder, output, basePath, value)
                  })
              _ = FilesUtils.cleanup(input.path)
            } yield list
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
