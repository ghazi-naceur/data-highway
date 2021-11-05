package gn.oss.data.highway.engine.sinks

import com.sksamuel.elastic4s.Response
import com.sksamuel.elastic4s.requests.bulk.BulkResponse
import com.sksamuel.elastic4s.requests.common.RefreshPolicy
import gn.oss.data.highway.configs.{ElasticUtils, HdfsUtils}
import gn.oss.data.highway.models
import gn.oss.data.highway.utils.{DataFrameUtils, FilesUtils, HdfsUtils, SharedUtils}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import cats.implicits._
import gn.oss.data.highway.models.DataHighwayRuntimeException.MustHaveFileSystemError
import gn.oss.data.highway.models.{
  DataHighwayErrorResponse,
  DataHighwaySuccessResponse,
  DataType,
  Elasticsearch,
  HDFS,
  Local,
  Storage,
  XLSX
}

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
    inputDataType match {
      case XLSX =>
        storage match {
          case HDFS =>
            HdfsUtils
              .listFilesRecursively(fs, input)
              .map(file => {
                DataFrameUtils
                  .loadDataFrame(inputDataType, file)
                  .map(df => indexDataFrameWithIndexQuery(df, output))
              })
          case Local =>
            FilesUtils
              .listFilesRecursively(new File(input), inputDataType.extension)
              .map(file => {
                DataFrameUtils
                  .loadDataFrame(inputDataType, file.getAbsolutePath)
                  .map(df => indexDataFrameWithIndexQuery(df, output))
              })
        }
      case _ =>
        DataFrameUtils
          .loadDataFrame(inputDataType, input)
          .map(df => indexDataFrameWithIndexQuery(df, output))
    }
    storage match {
      case HDFS  => HdfsUtils.movePathContent(fs, input, basePath)
      case Local => FilesUtils.movePathContent(input, s"$basePath/processed")
    }
  }

  /**
    * Index a DataFrame using an ES Index Query
    * @param df The DataFrame to be indexed
    * @param output The ES index
    */
  private def indexDataFrameWithIndexQuery(df: DataFrame, output: String): Unit = {
    DataFrameUtils
      .convertDataFrameToJsonLines(df)
      .map(_.map(line => indexDocInEs(output, line)))
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
    inputDataType match {
      case XLSX =>
        storage match {
          case HDFS =>
            HdfsUtils
              .listFilesRecursively(fs, input)
              .map(file => {
                DataFrameUtils
                  .loadDataFrame(inputDataType, file)
                  .map(df => {
                    indexDataFrameWithBulk(df, output)
                    HdfsUtils.movePathContent(fs, file, basePath)
                  })
              })
          case Local =>
            FilesUtils
              .listFilesRecursively(new File(input), inputDataType.extension)
              .map(file => {
                DataFrameUtils
                  .loadDataFrame(inputDataType, file.getAbsolutePath)
                  .map(df => {
                    indexDataFrameWithBulk(df, output)
                    FilesUtils
                      .movePathContent(file.getAbsolutePath, s"$basePath/processed/${file.getParentFile.getName}")
                  })
              })
        }
      case _ =>
        DataFrameUtils
          .loadDataFrame(inputDataType, input)
          .map(df => indexDataFrameWithBulk(df, output))
        storage match {
          case HDFS  => HdfsUtils.movePathContent(fs, input, basePath)
          case Local => FilesUtils.movePathContent(input, s"$basePath/processed")
        }
    }
  }

  /**
    * Index a DataFrame using an ES Bulk Query
    *
    * @param df The DataFrame to be indexed
    * @param output The ES index
    * @return Response of BulkResponse, otherwise a Throwable
    */
  private def indexDataFrameWithBulk(df: DataFrame, output: String): Either[Throwable, Response[BulkResponse]] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    DataFrameUtils
      .convertDataFrameToJsonLines(df)
      .map(lines => {
        val queries = lines.map(line => indexInto(output) doc line refresh RefreshPolicy.IMMEDIATE)
        esClient.execute { bulk(queries).refresh(RefreshPolicy.Immediate) }.await
      })
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
    * @return DataHighwayFileResponse, otherwise a DataHighwayErrorResponse
    */
  def handleElasticsearchChannel(
    input: models.File,
    output: Elasticsearch,
    storage: Option[Storage]
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    val basePath = new File(input.path).getParent
    storage match {
      case Some(value) =>
        value match {
          case HDFS =>
            val result = for {
              list <-
                HdfsUtils
                  .listFolders(fs, input.path)
                  .traverse(_.traverse(folder => sendToElasticsearch(input.dataType, folder, output, basePath, value)))
                  .flatten
              _ = HdfsUtils.cleanup(fs, input.path)
            } yield list
            SharedUtils.constructIOResponse(input, output, result)
          case Local =>
            val result = insertDocuments(input, output, basePath, value)
            FilesUtils.cleanup(input.path)
            SharedUtils.constructIOResponse(input, output, result)
        }
      case None => Left(MustHaveFileSystemError)
    }
  }

  /**
    * Inserts documents in Elasticsearch
    *
    * @param input The File input entity
    * @param output The Elasticsearch output entity
    * @param basePath The File entity base path
    * @param storage The input file system storage
    * @return List of Unit, otherwise a Throwable
    */
  def insertDocuments(
    input: models.File,
    output: Elasticsearch,
    basePath: String,
    storage: Storage
  ): Either[Throwable, List[Unit]] = {
    for {
      folders <- FilesUtils.listNonEmptyFoldersRecursively(input.path)
      res <-
        folders
          .filterNot(path => new File(path).listFiles.filter(_.isFile).toList.isEmpty)
          .traverse(folder => sendToElasticsearch(input.dataType, folder, output, basePath, storage))
    } yield res
  }
}
