package io.oss.data.highway.engine

import io.oss.data.highway.models.DataHighwayError.DataHighwayFileError
import io.oss.data.highway.models.{
  AdvancedOutput,
  Cassandra,
  Elasticsearch,
  File,
  HDFS,
  JSON,
  Kafka,
  KafkaConsumer,
  Local,
  Storage
}
import io.oss.data.highway.utils.DataFrameUtils.sparkSession
import io.oss.data.highway.utils.{FilesUtils, HdfsUtils}
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode
import cats.implicits._

import java.io
import java.util.UUID

object AdvancedSink extends HdfsUtils {

  val logger: Logger = Logger.getLogger(AdvancedSink.getClass.getName)

  /**
    * Cleanups the temporary folder
    *
    * @param output The tmp suffix path
    * @param storage The tmp file system storage
    * @return Serializable
    */
  private def cleanupTmp(output: String, storage: Option[Storage]): java.io.Serializable = {
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

  /**
    * Converts the temporary json data to the output dataset
    *
    * @param temporaryPath The temporary json path
    * @param tempoPathSuffix The suffix of the temporary json path
    * @param output The output File entity
    * @param storage The output file system storage
    * @param saveMode The output save mode
    * @return Serializable
    */
  def convertUsingBasicSink(
      temporaryPath: String,
      tempoPathSuffix: String,
      output: File,
      storage: Storage,
      saveMode: SaveMode
  ): java.io.Serializable = {
    val tempInputPath = new java.io.File(temporaryPath).getParent
    BasicSink.handleChannel(File(JSON, tempInputPath), output, Some(storage), saveMode)
    cleanupTmp(tempoPathSuffix, Some(storage))
  }

  /**
    * Extracts documents from Kafka topic and insert data into an Advanced Output
    *
    * @param kafka The input kafka entity
    * @param output The advanced output entity
    * @param storage The output file system storage
    * @param saveMode The output save mode
    */
  private def extractFromKafka(
      kafka: Kafka,
      output: AdvancedOutput,
      storage: Storage,
      saveMode: SaveMode
  ): java.io.Serializable = {
    val consumer = kafka.kafkaMode.asInstanceOf[Option[KafkaConsumer]]
    consumer match {
      case Some(value) =>
        output match {
          case cassandra @ Cassandra(_, _) =>
            val tempoPathSuffix =
              s"/tmp/data-highway/advanced-sink/cassandra/${System.currentTimeMillis().toString}/"
            val temporaryPath = tempoPathSuffix + UUID.randomUUID().toString
            val basePath      = new io.File(temporaryPath).getParent
            KafkaSampler.sinkWithSparkKafkaConnector(
              sparkSession,
              kafka,
              temporaryPath,
              storage,
              value.brokers,
              value.offset
            )
            CassandraSink.insertRows(
              File(JSON, temporaryPath),
              cassandra,
              basePath,
              saveMode
            )
            FilesUtils.delete(basePath)
          case elasticsearch @ Elasticsearch(_, _, _) =>
            val tempoPathSuffix =
              s"/tmp/data-highway/advanced-sink/elasticsearch/${System.currentTimeMillis().toString}/"
            val temporaryPath = tempoPathSuffix + UUID.randomUUID().toString
            val basePath      = new io.File(temporaryPath).getParent
            KafkaSampler.sinkWithSparkKafkaConnector(
              sparkSession,
              kafka,
              temporaryPath,
              storage,
              value.brokers,
              value.offset
            )
            ElasticSink
              .insertDocuments(
                File(JSON, temporaryPath),
                elasticsearch,
                new io.File(temporaryPath).getParent,
                storage
              )
            FilesUtils.delete(basePath)
        }
      case None =>
        new RuntimeException("Already taken care of")

    }
  }

  def handleRoute(
      input: Kafka,
      output: AdvancedOutput,
      saveMode: SaveMode,
      storage: Option[Storage]
  ): Either[Throwable, Any] = {
    Either.catchNonFatal {
      storage match {
        case Some(filesystem) =>
          extractFromKafka(input, output, filesystem, saveMode)
        case None =>
          new RuntimeException("Can't happen")
      }
    }
  }
}
