package io.oss.data.highway.converter

import java.time.Duration

import io.oss.data.highway.model.DataHighwayError.JsonError
import io.oss.data.highway.model.{DataHighwayError, DataType, KAFKA, Offset}
import io.oss.data.highway.utils.{
  DataFrameUtils,
  FilesUtils,
  KafkaTopicConsumer
}
import org.apache.spark.sql.SaveMode
import cats.implicits._
import io.oss.data.highway.configuration.SparkConfig
import io.oss.data.highway.utils.KafkaTopicConsumer.logger

import scala.jdk.CollectionConverters._

object JsonSink {

  /**
    * Converts file to json
    *
    * @param in The input data path
    * @param out The generated json file path
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @param sparkConfig The Spark Configuration
    * @return Unit if successful, otherwise Error
    */
  def convertToJson(in: String,
                    out: String,
                    saveMode: SaveMode,
                    inputDataType: DataType,
                    sparkConfig: SparkConfig): Either[JsonError, Unit] = {
    DataFrameUtils(sparkConfig)
      .loadDataFrame(in, inputDataType)
      .map(df => {
        df.coalesce(1)
          .write
          .mode(saveMode)
          .json(out)
      })
      .leftMap(thr =>
        JsonError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  def store(
      in: String,
      out: String,
      dataType: Option[DataType],
      brokerUrls: String,
      offset: Offset,
      consumerGroup: String): Either[DataHighwayError.KafkaError, Unit] = {
    for {
      consumed <- KafkaTopicConsumer.consume(in,
                                             brokerUrls,
                                             offset,
                                             consumerGroup)
      _ = while (true) {
        val record = consumed.poll(Duration.ofSeconds(5)).asScala
        logger.info("=======> Consumer :")
        for (data <- record.iterator) {
          logger.info(
            s"Topic: ${data.topic()}, Key: ${data.key()}, Value: ${data.value()}, " +
              s"Offset: ${data.offset()}, Partition: ${data.partition()}")
          val extension = dataType match {
            case Some(dataType) => dataType.`extension`
            case None           => KAFKA.`extension`
          }
          FilesUtils.save(out,
                          s"file-${System.currentTimeMillis()}.${extension}",
                          data.value())
        }
      }
    } yield ()
  }

  /**
    * Converts files to json
    *
    * @param in       The input data path
    * @param out      The generated json file path
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @param sparkConfig The Spark Configuration
    * @return List[Unit], otherwise Error
    */
  def handleJsonChannel(
      in: String,
      out: String,
      saveMode: SaveMode,
      inputDataType: DataType,
      sparkConfig: SparkConfig
  ): Either[DataHighwayError, List[Unit]] = {
    for {
      folders <- FilesUtils.listFoldersRecursively(in)
      list <- folders
        .traverse(folder => {
          val suffix =
            FilesUtils.reversePathSeparator(folder).split("/").last
          convertToJson(folder,
                        s"$out/$suffix",
                        saveMode,
                        inputDataType,
                        sparkConfig)
        })
        .leftMap(error =>
          JsonError(error.message, error.cause, error.stacktrace))
    } yield list
  }
}
