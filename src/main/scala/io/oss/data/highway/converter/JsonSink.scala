package io.oss.data.highway.converter

import java.io.File
import java.time.Duration

import io.oss.data.highway.model.DataHighwayError.JsonError
import io.oss.data.highway.model.{
  DataHighwayError,
  DataType,
  KAFKA,
  KafkaMode,
  KafkaStreaming,
  Offset,
  SimpleConsumer
}
import io.oss.data.highway.utils.{
  DataFrameUtils,
  FilesUtils,
  KafkaTopicConsumer
}
import org.apache.spark.sql.SaveMode
import cats.implicits._
import io.oss.data.highway.configuration.SparkConfig
import org.apache.kafka.streams.KafkaStreams
import org.apache.log4j.Logger

import scala.jdk.CollectionConverters._
import scala.util.Try

object JsonSink {

  val logger: Logger = Logger.getLogger(JsonSink.getClass)

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

  /**
    * Peeks data from a topic
    * @param in The input source topic
    * @param out The generated file
    * @param dataType The desired data type for the generated file (the extension)
    * @param kafkaMode The Kafka Mode
    * @param brokerUrls The kafka brokers urls
    * @param offset The consumer offset : Latest, Earliest, None
    * @param consumerGroup The consumer
    * @return a Unit, otherwise a Throwable
    */
  def store(in: String,
            out: String,
            dataType: Option[DataType],
            kafkaMode: KafkaMode,
            brokerUrls: String,
            offset: Offset,
            consumerGroup: String): Either[Throwable, Unit] = {
    val extension = dataType match {
      case Some(dataType) => dataType.`extension`
      case None           => KAFKA.`extension`
    }
    kafkaMode match {
      case SimpleConsumer =>
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

              FilesUtils.save(out,
                              s"file-${System.currentTimeMillis()}$extension",
                              data.value())
            }
          }
        } yield ()
      case KafkaStreaming(streamAppId) =>
        Try {
          new File(out).mkdirs()
          val kafkaStreamEntity =
            KafkaTopicConsumer.consumeWithStream(streamAppId,
                                                 in,
                                                 offset,
                                                 brokerUrls)
          kafkaStreamEntity.dataKStream.mapValues(
            data =>
              FilesUtils.save(out,
                              s"file-${System.currentTimeMillis()}$extension",
                              data))
          val streams = new KafkaStreams(kafkaStreamEntity.builder.build(),
                                         kafkaStreamEntity.props)

          streams.start()
        }.toEither
      case _ =>
        throw new RuntimeException(
          "This mode is not supported while reading data. The supported Kafka Consume Mode are " +
            ": 'SimpleConsumer' and 'KafkaStreaming'.")
    }
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
