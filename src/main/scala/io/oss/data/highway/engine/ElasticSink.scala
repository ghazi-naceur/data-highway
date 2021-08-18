package io.oss.data.highway.engine

import io.oss.data.highway.models.{DataType, Elasticsearch, HDFS, Local, Storage}
import io.oss.data.highway.utils.{DataFrameUtils, ElasticUtils, FilesUtils, HdfsUtils}
import org.apache.log4j.Logger
import cats.implicits._
import com.sksamuel.elastic4s.ElasticDsl.indexInto
import com.sksamuel.elastic4s.requests.common.RefreshPolicy
import io.oss.data.highway.models
import io.oss.data.highway.models.DataHighwayError.DataHighwayFileError

import java.io.File

object ElasticSink extends ElasticUtils with HdfsUtils {

  val logger: Logger = Logger.getLogger(ElasticSink.getClass.getName)

  /**
    * Indexes file's content into Elasticsearch
    *
    * @param inputDataType The input data type path
    * @param input The input data path
    * @param output The output Elasticsearch entity
    * @param basePath The base path for input, output and processed folders
    * @param storage The input file system storage
    * @return a Unit, otherwise an Error
    */
  private def sendToElasticsearch(
      inputDataType: DataType,
      input: String,
      output: Elasticsearch,
      basePath: String,
      storage: Storage
  ): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      if (!output.bulkEnabled)
        indexWithIndexQuery(inputDataType, input, output.index, basePath, storage)
      else
        indexWithBulkQuery(inputDataType, input, output.index, basePath, storage)
      logger.info(s"Successfully indexing data from '$input' into the '$output' index.")
    }
  }

  /**
    * Indexes data using an ES IndexQuery
    *
    * @param inputDataType The input data type path
    * @param input The input data folder
    * @param output The output Elasticsearch entity
    * @param basePath The base path of the input folder
    * @param storage The input file system storage : Local or HDFS
    * @return Any
    */
  private def indexWithIndexQuery(
      inputDataType: DataType,
      input: String,
      output: String,
      basePath: String,
      storage: Storage
  ): Any = {
    DataFrameUtils
      .loadDataFrame(inputDataType, input)
      .map(df => {
        val fieldNames = df.head().schema.fieldNames
        df.foreach(row => {
          val rowAsMap = row.getValuesMap(fieldNames)
          val line     = DataFrameUtils.toJson(rowAsMap)
          indexDocInEs(output, line)
        })
      })
    storage match {
      case HDFS =>
        HdfsUtils.movePathContent(fs, input, basePath)
      case Local =>
        FilesUtils.movePathContent(input, s"$basePath/processed")
    }
  }

  /**
    * Indexes data using an ES BulkQuery
    *
    * @param inputDataType The input data type path
    * @param input The input data folder
    * @param output The ES index
    * @param basePath The base path of the input folder
    * @param storage The input file system storage : Local or HDFS
    * @return Any
    */
  private def indexWithBulkQuery(
      inputDataType: DataType,
      input: String,
      output: String,
      basePath: String,
      storage: Storage
  ): Any = {
    import com.sksamuel.elastic4s.ElasticDsl._
    import scala.collection.JavaConverters._
    import DataFrameUtils.sparkSession.implicits._

    DataFrameUtils
      .loadDataFrame(inputDataType, input)
      .map(df => {
        val fieldNames = df.head().schema.fieldNames
        val lines = df
          .map(row => {
            val rowAsMap = row.getValuesMap(fieldNames)
            DataFrameUtils.toJson(rowAsMap)
          })
          .collectAsList()
          .asScala
          .toList
        val queries = lines.map(line => indexInto(output) doc line refresh RefreshPolicy.IMMEDIATE)
        esClient.execute {
          bulk(queries).refresh(RefreshPolicy.Immediate)
        }.await
      })

    storage match {
      case HDFS =>
        HdfsUtils.movePathContent(fs, input, basePath)
      case Local =>
        FilesUtils.movePathContent(input, s"$basePath/processed")
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
                      sendToElasticsearch(input.dataType, folder, output, basePath, value)
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
                    sendToElasticsearch(input.dataType, folder, output, basePath, value)
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
