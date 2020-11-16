package io.oss.data.highway.converter

import java.io.File
import java.time.Duration

import io.oss.data.highway.model.{
  DataHighwayError,
  DataType,
  KAFKA,
  KafkaMode,
  KafkaStreaming,
  Offset,
  SimpleConsumer,
  SparkKafkaConsumerPlugin
}
import io.oss.data.highway.utils.{
  DataFrameUtils,
  FilesUtils,
  KafkaTopicConsumer
}
import org.apache.spark.sql.functions.to_json
import io.oss.data.highway.configuration.SparkConfig
import org.apache.kafka.streams.KafkaStreams
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.struct

import scala.jdk.CollectionConverters._
import scala.util.Try

/**
  * The aim of this object is to get data samples, from Kafka topics, and store it in files.
  */
object KafkaSampler {

  val logger: Logger = Logger.getLogger(KafkaSampler.getClass)

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
  def peek(in: String,
           out: String,
           dataType: Option[DataType],
           kafkaMode: KafkaMode,
           brokerUrls: String,
           offset: Offset,
           consumerGroup: String,
           sparkConfig: SparkConfig): Either[Throwable, Unit] = {
    val extension = dataType match {
      case Some(dataType) => dataType.`extension`
      case None           => KAFKA.`extension`
    }
    kafkaMode match {
      case SimpleConsumer =>
        sinkWithSimpleConsumer(in,
                               out,
                               brokerUrls,
                               offset,
                               consumerGroup,
                               extension)
      case KafkaStreaming(streamAppId) =>
        sinkWithKafkaStreams(in,
                             out,
                             brokerUrls,
                             offset,
                             extension,
                             streamAppId)
      case SparkKafkaConsumerPlugin(useStream) =>
        val session = DataFrameUtils(sparkConfig).sparkSession
        Try {
          if (useStream) {
            sinkWithSparkKafkaStreamingPlugin(session,
                                              in,
                                              out,
                                              brokerUrls,
                                              offset,
                                              extension)
          } else {
            sinkWithSparkKafkaPlugin(session,
                                     in,
                                     out,
                                     brokerUrls,
                                     offset,
                                     extension)
          }
        }.toEither
      case _ =>
        throw new RuntimeException("This mode is not supported while reading data. The supported Kafka Consume Mode are " +
          ": 'SimpleConsumer', 'KafkaStreaming' and 'SparkKafkaConsumerPlugin'.")
    }
  }

  private def sinkWithSparkKafkaPlugin(session: SparkSession,
                                       in: String,
                                       out: String,
                                       brokerUrls: String,
                                       offset: Offset,
                                       extension: String): Unit = {
    import session.implicits._
    session.read
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerUrls)
      .option("startingOffsets", offset.value)
      .option("subscribe", in)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .select(to_json(struct("value")))
      .toJavaRDD
      .foreach(data =>
        FilesUtils.save(
          out,
          s"spark-kafka-plugin-${System.currentTimeMillis()}$extension",
          data.toString()))
    logger.info(
      s"Successfully sinking '$`extension`' data provided by the input topic '$in' in the output folder '$out/spark-kafka-plugin-*****$extension'")
  }

  private def sinkWithSparkKafkaStreamingPlugin(session: SparkSession,
                                                in: String,
                                                out: String,
                                                brokerUrls: String,
                                                offset: Offset,
                                                extension: String): Unit = {
    import session.implicits._
    session.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerUrls)
      .option("startingOffsets", offset.value)
      .option("subscribe", in)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .select("value")
      .writeStream
      .format(extension.substring(1)) // todo : Hideous ! Remove "." from the extension
      .option(
        "path",
        s"$out/spark-kafka-streaming-plugin-${System.currentTimeMillis()}")
      .option("checkpointLocation",
              s"/tmp/checkpoint/${System.currentTimeMillis()}")
      .start()
      .awaitTermination()
    logger.info(
      s"Successfully sinking '$`extension`' data provided by the input topic '$in' in the output folder '$out/spark-kafka-streaming-plugin-*****$extension'")
  }

  private def sinkWithKafkaStreams(
      in: String,
      out: String,
      brokerUrls: String,
      offset: Offset,
      extension: String,
      streamAppId: String): Either[Throwable, Unit] = {
    Try {
      new File(out).mkdirs()
      val kafkaStreamEntity =
        KafkaTopicConsumer.consumeWithStream(streamAppId,
                                             in,
                                             offset,
                                             brokerUrls)
      kafkaStreamEntity.dataKStream.mapValues(
        data => {
          FilesUtils
            .save(out,
                  s"kafka-streams-${System.currentTimeMillis()}$extension",
                  data)
          logger.info(
            s"Successfully sinking '$`extension`' data provided by the input topic '$in' in the output folder '$out/kafka-streams-*****$extension'")
        }
      )
      val streams = new KafkaStreams(kafkaStreamEntity.builder.build(),
                                     kafkaStreamEntity.props)
      streams.start()
    }.toEither
  }

  private def sinkWithSimpleConsumer(
      in: String,
      out: String,
      brokerUrls: String,
      offset: Offset,
      consumerGroup: String,
      extension: String): Either[DataHighwayError.KafkaError, Unit] = {
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

          FilesUtils.save(
            out,
            s"simple-consumer-${System.currentTimeMillis()}$extension",
            data.value())
          logger.info(
            s"Successfully sinking '$`extension`' data provided by the input topic '$in' in the output folder '$out/simple-consumer-*****$extension'")
        }
      }
    } yield ()
  }
}
