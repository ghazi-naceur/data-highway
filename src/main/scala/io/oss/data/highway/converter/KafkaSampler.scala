package io.oss.data.highway.converter

import java.io.File
import java.time.Duration
import java.util.UUID
import io.oss.data.highway.model.{
  AVRO,
  DataHighwayError,
  DataType,
  Earliest,
  JSON,
  KafkaMode,
  Offset,
  PureKafkaConsumer,
  PureKafkaStreamsConsumer,
  SparkKafkaConsumerPlugin
}
import io.oss.data.highway.utils.{
  DataFrameUtils,
  FilesUtils,
  KafkaTopicConsumer,
  KafkaUtils
}
import org.apache.spark.sql.functions.to_json
import io.oss.data.highway.configuration.SparkConfigs
import org.apache.kafka.streams.KafkaStreams
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.struct
import monix.execution.Scheduler.{global => scheduler}

import scala.concurrent.duration._
import cats.implicits._

import scala.jdk.CollectionConverters._
import scala.util.Try

/**
  * The aim of this object is to get data samples, from Kafka topics, and store it in files.
  */
object KafkaSampler {

  val logger: Logger = Logger.getLogger(KafkaSampler.getClass)

  /**
    * Consumes data from a topic
    * @param in The input source topic
    * @param out The generated file
    * @param dataType The desired data type for the generated file (the extension)
    * @param kafkaMode The Kafka Mode
    * @param brokers The kafka brokers urls
    * @param offset The consumer offset : Latest, Earliest, None
    * @param consGroup The consumer group
    * @return a Unit, otherwise a Throwable
    */
  def consumeFromTopic(in: String,
                       out: String,
                       dataType: Option[DataType],
                       kafkaMode: KafkaMode,
                       brokers: String,
                       offset: Offset,
                       consGroup: String,
                       sparkConfig: SparkConfigs): Either[Throwable, Unit] = {
    KafkaUtils.verifyTopicExistence(in, brokers, enableTopicCreation = false)
    val ext = computeOutputExtension(dataType)
    kafkaMode match {
      case PureKafkaStreamsConsumer(streamAppId) =>
        sinkWithPureKafkaStreams(in, out, brokers, offset, ext, streamAppId)

      case PureKafkaConsumer =>
        Either.catchNonFatal(
          scheduler.scheduleWithFixedDelay(0.seconds, 3.seconds) {
            sinkWithPureKafka(in, out, brokers, offset, consGroup, ext)
          })

      case SparkKafkaConsumerPlugin(useStream) =>
        val sess = DataFrameUtils(sparkConfig).sparkSession
        Try {
          if (useStream) {
            sinkWithSparkKafkaStreamsPlugin(sess, in, out, brokers, offset, ext)
          } else {
            sinkWithSparkKafkaPlugin(sess, in, out, brokers, offset, ext)
          }
        }.toEither
      case _ =>
        throw new RuntimeException(
          s"This mode is not supported while reading data. The supported Kafka Consume Mode are : '${PureKafkaConsumer.getClass.getName}' and '${SparkKafkaConsumerPlugin.getClass.getName}'.")
    }
  }

  /**
    * Computes the generated output files extension based on the output DataType
    * @param dataType The output files DataType
    * @return The output extension
    */
  private def computeOutputExtension(dataType: Option[DataType]): String = {
    dataType match {
      case optDataType @ Some(dataType)
          if optDataType.contains(JSON) || optDataType.contains(AVRO) =>
        dataType.extension
      case None => JSON.extension
      case _    => JSON.extension
    }
  }

  /**
    * Sinks topic content into files using a Spark-Kafka-Plugin Consumer
    *
    * @param session The Spark session
    * @param in The input kafka topic
    * @param out The output folder
    * @param brokerUrls The kafka brokers urls
    * @param offset The kafka consumer offset
    * @param extension The output files extension
    */
  private def sinkWithSparkKafkaPlugin(session: SparkSession,
                                       in: String,
                                       out: String,
                                       brokerUrls: String,
                                       offset: Offset,
                                       extension: String): Unit = {
    import session.implicits._
    var mutableOffset = offset
    if (mutableOffset != Earliest) {
      logger.warn(
        s"Starting offset can't be ${mutableOffset.value} for batch queries on Kafka. So, we'll set it at 'Earliest'.")
      mutableOffset = Earliest
    }
    logger.info(
      s"Starting to sink '$extension' data provided by the input topic '$in' in the output folder pattern '$out/spark-kafka-plugin-*****$extension'")
    session.read
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerUrls)
      .option("startingOffsets", mutableOffset.value)
      .option("subscribe", in)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .select(to_json(struct("value")))
      .toJavaRDD
      .foreach(data =>
        FilesUtils.save(
          out,
          s"spark-kafka-plugin-${UUID.randomUUID()}-${System.currentTimeMillis()}.$extension",
          data.toString()))
  }

  /**
    * Sinks topic content into files using a Spark-Kafka-Plugin Streaming Consumer
    *
    * @param session The Spark session
    * @param in The input kafka topic
    * @param out The output folder
    * @param brokerUrls The kafka brokers urls
    * @param offset The kafka consumer offset
    * @param extension The output files extension
    */
  private def sinkWithSparkKafkaStreamsPlugin(session: SparkSession,
                                              in: String,
                                              out: String,
                                              brokerUrls: String,
                                              offset: Offset,
                                              extension: String): Unit = {
    import session.implicits._
    logger.info(
      s"Starting to sink '$extension' data provided by the input topic '$in' in the output folder pattern '$out/spark-kafka-streaming-plugin-*****$extension'")
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
      .format(extension)
      .option(
        "path",
        s"$out/spark-kafka-streaming-plugin-${System.currentTimeMillis()}")
      .option(
        "checkpointLocation",
        s"/tmp/checkpoint/${UUID.randomUUID()}-${System.currentTimeMillis()}")
      .start()
      .awaitTermination()
  }

  /**
    * Sinks topic content into files using a Pure Kafka Streaming Consumer
    *
    * @param in The input kafka topic
    * @param out The output folder
    * @param brokerUrls The kafka brokers urls
    * @param offset The kafka consumer offset
    * @param extension The output files extension
    * @param streamAppId The identifier of the streaming application
    */
  private def sinkWithPureKafkaStreams(
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
          val uuid: String =
            s"${UUID.randomUUID()}-${System.currentTimeMillis()}"
          FilesUtils
            .save(out, s"kafka-streams-$uuid.$extension", data)
          logger.info(
            s"Successfully sinking '$extension' data provided by the input topic '$in' in the output folder pattern '$out/kafka-streams-*****$extension'")
        }
      )
      val streams = new KafkaStreams(kafkaStreamEntity.builder.build(),
                                     kafkaStreamEntity.props)
      streams.start()
    }.toEither
  }

  /**
    * Sinks topic content into files using Pure Kafka Consumer
    *
    * @param in The input kafka topic
    * @param out The output folder
    * @param brokerUrls The kafka brokers urls
    * @param offset The kafka consumer offset
    * @param consumerGroup The consumer group
    * @param extension The output files extension
    */
  private def sinkWithPureKafka(
      in: String,
      out: String,
      brokerUrls: String,
      offset: Offset,
      consumerGroup: String,
      extension: String): Either[DataHighwayError.KafkaError, Unit] = {
    // todo offset == none => Undefined offset with no reset policy for partitions:
    KafkaTopicConsumer
      .consume(in, brokerUrls, offset, consumerGroup)
      .map(consumed => {
        val record = consumed.poll(Duration.ofSeconds(5)).asScala
        for (data <- record.iterator) {
          logger.info(
            s"Topic: ${data.topic()}, Key: ${data.key()}, Value: ${data.value()}, " +
              s"Offset: ${data.offset()}, Partition: ${data.partition()}")
          val uuid: String =
            s"${UUID.randomUUID()}-${System.currentTimeMillis()}"
          FilesUtils
            .save(out, s"simple-consumer-$uuid.$extension", data.value())
          logger.info(
            s"Successfully sinking '$extension' data provided by the input topic '$in' in the output folder pattern '$out/simple-consumer-*****$extension'")
        }
        consumed.close() // Close it to rejoin again while rescheduling
      })
  }
}
