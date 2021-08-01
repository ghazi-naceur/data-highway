package io.oss.data.highway.sinks

import java.time.Duration
import java.util.UUID
import io.oss.data.highway.models.{
  Earliest,
  HDFS,
  JSON,
  KafkaMode,
  Local,
  Offset,
  PureKafkaConsumer,
  PureKafkaStreamsConsumer,
  SparkKafkaPluginConsumer,
  SparkKafkaPluginStreamsConsumer,
  Storage
}
import io.oss.data.highway.utils.{FilesUtils, HdfsUtils, KafkaTopicConsumer, KafkaUtils}
import org.apache.spark.sql.functions.to_json
import org.apache.kafka.streams.KafkaStreams
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.struct
import monix.execution.Scheduler.{global => scheduler}

import scala.concurrent.duration._
import cats.implicits._
import io.oss.data.highway.models.DataHighwayError.KafkaError
import io.oss.data.highway.utils.DataFrameUtils.sparkSession
import org.apache.hadoop.fs.FileSystem

import scala.jdk.CollectionConverters._
import scala.util.Try

/**
  * The aim of this object is to get data records samples, from Kafka topics, and store it in files.
  */
object KafkaSampler extends HdfsUtils {

  val logger: Logger = Logger.getLogger(KafkaSampler.getClass.getName)

  /**
    * Consumes data from a topic
    *
    * @param inputTopic The input source topic
    * @param outputPath The output path
    * @param storage The output file system storage
    * @param kafkaMode The Kafka Mode
    * @return a Unit, otherwise a Throwable
    */
  def consumeFromTopic(
      inputTopic: String,
      outputPath: String,
      storage: Storage,
      kafkaMode: KafkaMode
  ): Either[Throwable, Unit] = {
    KafkaUtils.verifyTopicExistence(inputTopic, kafkaMode.brokers, enableTopicCreation = false)

    kafkaMode match {
      case PureKafkaStreamsConsumer(brokers, streamAppId, offset) =>
        sinkWithPureKafkaStreams(inputTopic, outputPath, storage, brokers, offset, streamAppId, fs)
      case PureKafkaConsumer(brokers, consGroup, offset) =>
        // todo repetitive re-subscription ===> maybe make it one-shot job
        // todo start the app with a new "consumer-group"
        Either.catchNonFatal(scheduler.scheduleWithFixedDelay(0.seconds, 3.seconds) {
          sinkWithPureKafka(inputTopic, outputPath, storage, brokers, offset, consGroup, fs)
        })
      case SparkKafkaPluginStreamsConsumer(brokers, offset) =>
        Either.catchNonFatal {
          val thread = new Thread(() => {
            sinkViaSparkKafkaStreamsPlugin(
              sparkSession,
              inputTopic,
              outputPath,
              storage,
              brokers,
              offset
            )
          })
          thread.start()
        }
      case SparkKafkaPluginConsumer(brokers, offset) =>
        // one-shot job
        Either.catchNonFatal(
          sinkViaSparkKafkaPlugin(sparkSession, inputTopic, outputPath, storage, brokers, offset)
        )
      case _ =>
        throw new RuntimeException(
          s"This mode is not supported while consuming data. The provided input kafka mode is : '$kafkaMode'"
        )
    }
  }

  /**
    * Sinks topic content into files using a [[io.oss.data.highway.models.SparkKafkaPluginConsumer]]
    *
    * @param session The Spark session
    * @param inputTopic The input kafka topic
    * @param outputPath The output folder
    * @param storage The output file system storage
    * @param brokerUrls The kafka brokers urls
    * @param offset The kafka consumer offset
    * @return Unit
    */
  private[sinks] def sinkViaSparkKafkaPlugin(
      session: SparkSession,
      inputTopic: String,
      outputPath: String,
      storage: Storage,
      brokerUrls: String,
      offset: Offset
  ): Unit = {
    import session.implicits._
    var mutableOffset = offset
    if (mutableOffset != Earliest) {
      logger.warn(
        s"Starting offset can't be ${mutableOffset.value} for batch queries on Kafka. So, we'll set it at 'Earliest'."
      )
      mutableOffset = Earliest
    }
    logger.info(
      s"Starting to sink '${JSON.extension}' data provided by the input topic '$inputTopic' in the output folder pattern" +
        s" '$outputPath/spark-kafka-plugin-*****${JSON.extension}'"
    )
    session.read
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerUrls)
      .option("startingOffsets", mutableOffset.value)
      .option("subscribe", inputTopic)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .select(to_json(struct("value")))
      .toJavaRDD
      .foreach(data =>
        storage match {
          case Local =>
            FilesUtils.createFile(
              outputPath,
              s"spark-kafka-plugin-${UUID.randomUUID()}-${System.currentTimeMillis()}.${JSON.extension}",
              data.toString()
            )
          case HDFS =>
            HdfsUtils.save(
              fs,
              s"$outputPath/spark-kafka-plugin-${UUID.randomUUID()}-${System.currentTimeMillis()}.${JSON.extension}",
              data.toString()
            )
        }
      )
  }

  /**
    * Sinks topic content into files using a [[io.oss.data.highway.models.SparkKafkaPluginStreamsConsumer]]
    *
    * @param session The Spark session
    * @param inputTopic The input kafka topic
    * @param outputPath The output folder
    * @param storage The output file system storage
    * @param brokerUrls The kafka brokers urls
    * @param offset The kafka consumer offset
    * @return Unit
    */
  private[sinks] def sinkViaSparkKafkaStreamsPlugin(
      session: SparkSession,
      inputTopic: String,
      outputPath: String,
      storage: Storage,
      brokerUrls: String,
      offset: Offset
  ): Unit = {
    import session.implicits._
    logger.info(
      s"Starting to sink '${JSON.extension}' data provided by the input topic '$inputTopic' in the output folder pattern " +
        s"'$outputPath/spark-kafka-streaming-plugin-*****${JSON.extension}'"
    )
    val stream = session.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerUrls)
      .option("startingOffsets", offset.value)
      .option("subscribe", inputTopic)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .select("value")
      .writeStream
      .format(JSON.extension)
      .option("path", s"$outputPath/spark-kafka-streaming-plugin-${System.currentTimeMillis()}")
    storage match {
      case Local =>
        stream
          .option(
            "checkpointLocation",
            s"/tmp/checkpoint/${UUID.randomUUID()}-${System.currentTimeMillis()}"
          )
          .start()
          .awaitTermination()
      case HDFS =>
        stream
          .option(
            "checkpointLocation",
            s"${hadoopConf.host}/tmp/checkpoint/${UUID.randomUUID()}-${System.currentTimeMillis()}"
          )
          .start()
          .awaitTermination()
    }
  }

  /**
    * Sinks topic content into files using a [[io.oss.data.highway.models.PureKafkaStreamsConsumer]]
    *
    * @param inputTopic The input kafka topic
    * @param outputPath The output folder
    * @param storage The output file system storage
    * @param brokerUrls The kafka brokers urls
    * @param offset The kafka consumer offset
    * @param streamAppId The identifier of the streaming application
    * @return a Unit, otherwise an Error
    */
  private[sinks] def sinkWithPureKafkaStreams(
      inputTopic: String,
      outputPath: String,
      storage: Storage,
      brokerUrls: String,
      offset: Offset,
      streamAppId: String,
      fs: FileSystem
  ): Either[Throwable, Unit] = {
    Try {
      val kafkaStreamEntity =
        KafkaTopicConsumer.consumeWithStream(streamAppId, inputTopic, offset, brokerUrls)
      kafkaStreamEntity.dataKStream.mapValues(data => {
        val uuid: String = s"${UUID.randomUUID()}-${System.currentTimeMillis()}"
        storage match {
          case Local =>
            FilesUtils.createFile(outputPath, s"kafka-streams-$uuid.${JSON.extension}", data)
          case HDFS =>
            HdfsUtils.save(fs, s"$outputPath/kafka-streams-$uuid.${JSON.extension}", data)
        }
        logger.info(
          s"Successfully sinking '${JSON.extension}' data provided by the input topic '$inputTopic' in the output folder pattern " +
            s"'$outputPath/kafka-streams-*****${JSON.extension}'"
        )
      })
      val streams = new KafkaStreams(kafkaStreamEntity.builder.build(), kafkaStreamEntity.props)
      streams.start()
    }.toEither
  }

  /**
    * Sinks topic content into files using a [[io.oss.data.highway.models.PureKafkaConsumer]]
    *
    * @param inputTopic The input kafka topic
    * @param outputPath The output folder
    * @param storage The output file system storage
    * @param brokerUrls The kafka brokers urls
    * @param offset The kafka consumer offset
    * @param consumerGroup The consumer group
    * @param fs The provided File System
    * @return a Unit, otherwise an Error
    */
  private[sinks] def sinkWithPureKafka(
      inputTopic: String,
      outputPath: String,
      storage: Storage,
      brokerUrls: String,
      offset: Offset,
      consumerGroup: String,
      fs: FileSystem
  ): Either[KafkaError, Unit] = {
    KafkaTopicConsumer
      .consume(inputTopic, brokerUrls, offset, consumerGroup)
      .map(consumed => {
        val record = consumed.poll(Duration.ofSeconds(5)).asScala
        for (data <- record.iterator) {
          logger.info(
            s"Topic: ${data.topic()}, Key: ${data.key()}, Value: ${data.value()}, " +
              s"Offset: ${data.offset()}, Partition: ${data.partition()}"
          )
          val uuid: String =
            s"${UUID.randomUUID()}-${System.currentTimeMillis()}"
          storage match {
            case Local =>
              FilesUtils
                .createFile(outputPath, s"simple-consumer-$uuid.${JSON.extension}", data.value())
            case HDFS =>
              HdfsUtils
                .save(fs, s"$outputPath/simple-consumer-$uuid.${JSON.extension}", data.value())
          }
          logger.info(
            s"Successfully sinking '${JSON.extension}' data provided by the input topic '$inputTopic' in the output folder pattern " +
              s"'$outputPath/simple-consumer-*****${JSON.extension}'"
          )
        }
        consumed.close() // Close it to rejoin again while rescheduling
      })
  }
}
