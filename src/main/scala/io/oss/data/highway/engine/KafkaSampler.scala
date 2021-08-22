package io.oss.data.highway.engine

import java.time.Duration
import java.util.UUID
import io.oss.data.highway.models.{
  Earliest,
  File,
  HDFS,
  JSON,
  Kafka,
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
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.struct
import monix.execution.Scheduler.{global => scheduler}
import cats.implicits._
import io.oss.data.highway.models.DataHighwayError.{DataHighwayFileError, KafkaError}
import io.oss.data.highway.utils.DataFrameUtils.sparkSession
import org.apache.hadoop.fs.FileSystem

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

/**
  * The aim of this object is to get data records samples, from Kafka topics, and store it in files.
  */
object KafkaSampler extends HdfsUtils {

  val logger: Logger = Logger.getLogger(KafkaSampler.getClass.getName)

  /**
    * Consumes data from a topic
    *
    * @param input The input Kafka entity
    * @param output The output File entity
    * @param storage The output file system storage
    * @return a Unit, otherwise a Throwable
    */
  def consumeFromTopic(
      input: Kafka,
      output: io.oss.data.highway.models.File,
      saveMode: SaveMode,
      storage: Option[Storage]
  ): Either[Throwable, Unit] = {
    val tempoPathSuffix = "/tmp/data-highway-kafka/"
    val temporaryPath   = tempoPathSuffix + UUID.randomUUID().toString
    (storage, input.kafkaMode) match {
      case (Some(filesystem), Some(km)) =>
        KafkaUtils.verifyTopicExistence(input.topic, km.brokers, enableTopicCreation = false)
        km match {
          case PureKafkaConsumer(brokers, consGroup, offset) =>
            Either.catchNonFatal(scheduler.scheduleWithFixedDelay(0.seconds, 5.seconds) {
              sinkWithPureKafka(
                input.topic,
                temporaryPath,
                tempoPathSuffix,
                output,
                filesystem,
                saveMode,
                brokers,
                offset,
                consGroup,
                fs
              )
            })
          case PureKafkaStreamsConsumer(brokers, streamAppId, offset) =>
            sinkWithPureKafkaStreams(
              input.topic,
              temporaryPath,
              tempoPathSuffix,
              output,
              filesystem,
              saveMode,
              brokers,
              offset,
              streamAppId,
              fs
            )
          case SparkKafkaPluginConsumer(brokers, offset) =>
            // one-shot job
            Either.catchNonFatal {
              sinkViaSparkKafkaPlugin(
                sparkSession,
                input.topic,
                temporaryPath,
                tempoPathSuffix,
                output,
                filesystem,
                saveMode,
                brokers,
                offset
              )
            }
          case SparkKafkaPluginStreamsConsumer(brokers, offset) =>
            // only json data type support
            Either.catchNonFatal {
              val thread = new Thread(() => {
                sinkViaSparkKafkaStreamsPlugin(
                  sparkSession,
                  input.topic,
                  output.path,
                  filesystem,
                  brokers,
                  offset
                )
              })
              thread.start()
            }
          case _ =>
            throw new RuntimeException(
              s"This mode is not supported while consuming Kafka topics. The supported modes are " +
                s"${PureKafkaConsumer.getClass}, ${SparkKafkaPluginConsumer.getClass}, ${PureKafkaStreamsConsumer.getClass}" +
                s"and ${SparkKafkaPluginStreamsConsumer.getClass}. The provided input kafka mode is '$km'."
            )
        }
      case _ =>
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
    * Sinks topic content into files using a [[io.oss.data.highway.models.SparkKafkaPluginConsumer]]
    *
    * @param session The Spark session
    * @param inputTopic The input kafka topic
    * @param temporaryPath The temporary output folder
    * @param tempoPathSuffix The suffix of the temporary output folder
    * @param output The output File entity
    * @param storage The output file system storage
    * @param saveMode The output save mode
    * @param brokerUrls The kafka brokers urls
    * @param offset The kafka consumer offset
    */
  private[engine] def sinkViaSparkKafkaPlugin(
      session: SparkSession,
      inputTopic: String,
      temporaryPath: String,
      tempoPathSuffix: String,
      output: io.oss.data.highway.models.File,
      storage: Storage,
      saveMode: SaveMode,
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
        s" '$temporaryPath/spark-kafka-plugin-*****${JSON.extension}'"
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
              temporaryPath,
              s"spark-kafka-plugin-${UUID.randomUUID()}-${System.currentTimeMillis()}.${JSON.extension}",
              data.toString()
            )
          case HDFS =>
            HdfsUtils.save(
              fs,
              s"$temporaryPath/spark-kafka-plugin-${UUID.randomUUID()}-${System
                .currentTimeMillis()}.${JSON.extension}",
              data.toString()
            )
        }
      )
    convertUsingBasicSink(temporaryPath, tempoPathSuffix, output, storage, saveMode)
  }

  /**
    * Sinks topic content into files using a [[io.oss.data.highway.models.SparkKafkaPluginStreamsConsumer]]
    *
    * @param session The Spark session
    * @param inputTopic The input kafka topic
    * @param storage The output file system storage
    * @param brokerUrls The kafka brokers urls
    * @param offset The kafka consumer offset
    */
  private[engine] def sinkViaSparkKafkaStreamsPlugin(
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
    * @param temporaryPath The temporary output folder
    * @param tempoPathSuffix The suffix of the temporary output folder
    * @param output The output File entity
    * @param storage The output file system storage
    * @param saveMode The output save mode
    * @param brokerUrls The kafka brokers urls
    * @param offset The kafka consumer offset
    * @param streamAppId The identifier of the streaming application
    * @param fs The output file system entity
    * @return Unit, otherwise a Throwable
    */
  private[engine] def sinkWithPureKafkaStreams(
      inputTopic: String,
      temporaryPath: String,
      tempoPathSuffix: String,
      output: io.oss.data.highway.models.File,
      storage: Storage,
      saveMode: SaveMode,
      brokerUrls: String,
      offset: Offset,
      streamAppId: String,
      fs: FileSystem
  ): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      val kafkaStreamEntity =
        KafkaTopicConsumer.consumeWithStream(streamAppId, inputTopic, offset, brokerUrls)
      kafkaStreamEntity.dataKStream.mapValues(data => {
        val uuid: String = s"${UUID.randomUUID()}-${System.currentTimeMillis()}"
        storage match {
          case Local =>
            FilesUtils.createFile(temporaryPath, s"kafka-streams-$uuid.${JSON.extension}", data)
          case HDFS =>
            HdfsUtils.save(fs, s"$temporaryPath/kafka-streams-$uuid.${JSON.extension}", data)
        }
        logger.info(
          s"Successfully sinking '${JSON.extension}' data provided by the input topic '$inputTopic' in the output folder pattern " +
            s"'$temporaryPath/kafka-streams-*****${JSON.extension}'"
        )
        convertUsingBasicSink(temporaryPath, tempoPathSuffix, output, storage, saveMode)
      })
      val streams = new KafkaStreams(kafkaStreamEntity.builder.build(), kafkaStreamEntity.props)
      streams.start()
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
  private def convertUsingBasicSink(
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
    * Sinks topic content into files using a [[io.oss.data.highway.models.PureKafkaConsumer]]
    *
    * @param inputTopic The input kafka topic
    * @param temporaryPath The temporary output folder
    * @param tempoPathSuffix The suffix of the temporary output folder
    * @param output The output File entity
    * @param storage The output file system storage
    * @param saveMode The output save mode
    * @param brokerUrls The kafka brokers urls
    * @param offset The kafka consumer offset
    * @param consumerGroup The consumer group name
    * @param fs The output file system entity
    * @return Unit, otherwise a Throwable
    */
  private[engine] def sinkWithPureKafka(
      inputTopic: String,
      temporaryPath: String,
      tempoPathSuffix: String,
      output: io.oss.data.highway.models.File,
      storage: Storage,
      saveMode: SaveMode,
      brokerUrls: String,
      offset: Offset,
      consumerGroup: String,
      fs: FileSystem
  ): Either[KafkaError, Unit] = {
    KafkaTopicConsumer
      .consume(inputTopic, brokerUrls, offset, consumerGroup)
      .map(consumed => {
        val record = consumed.poll(Duration.ofSeconds(1)).asScala
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
                .createFile(temporaryPath, s"simple-consumer-$uuid.${JSON.extension}", data.value())
            case HDFS =>
              HdfsUtils
                .save(fs, s"$temporaryPath/simple-consumer-$uuid.${JSON.extension}", data.value())
          }
          logger.info(
            s"Successfully sinking '${JSON.extension}' data provided by the input topic '$inputTopic' in the output folder pattern " +
              s"'$temporaryPath/simple-consumer-*****${JSON.extension}'"
          )
        }
        convertUsingBasicSink(temporaryPath, tempoPathSuffix, output, storage, saveMode)
        consumed.close()
      })
  }

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
}
