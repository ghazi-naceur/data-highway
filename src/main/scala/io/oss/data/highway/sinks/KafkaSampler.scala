package io.oss.data.highway.sinks

import java.time.Duration
import java.util.UUID
import io.oss.data.highway.models.{
  AVRO,
  DataType,
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
import io.oss.data.highway.utils.{
  DataFrameUtils,
  FilesUtils,
  HdfsUtils,
  KafkaTopicConsumer,
  KafkaUtils
}
import org.apache.spark.sql.functions.to_json
import org.apache.kafka.streams.KafkaStreams
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.struct
import monix.execution.Scheduler.{global => scheduler}

import scala.concurrent.duration._
import cats.implicits._
import io.oss.data.highway.models.DataHighwayError.KafkaError
import org.apache.hadoop.fs.FileSystem

import scala.jdk.CollectionConverters._
import scala.util.Try

/**
  * The aim of this object is to get data samples, from Kafka topics, and store it in files.
  */
object KafkaSampler extends HdfsUtils {

  val logger: Logger = Logger.getLogger(KafkaSampler.getClass)

  /**
    * Consumes data from a topic
    *
    * @param in The input source topic
    * @param out The output path
    * @param storage The output file system storage
    * @param kafkaMode The Kafka Mode
    * @return a Unit, otherwise a Throwable
    */
  def consumeFromTopic(
      in: String,
      out: String,
      storage: Storage,
      kafkaMode: KafkaMode
  ): Either[Throwable, Unit] = {
    KafkaUtils.verifyTopicExistence(in, kafkaMode.brokers, enableTopicCreation = false)
    val ext = computeOutputExtension(kafkaMode.dataType)
    kafkaMode match {

      case PureKafkaStreamsConsumer(brokers, streamAppId, offset, _) =>
        sinkWithPureKafkaStreams(in, out, storage, brokers, offset, ext, streamAppId, fs)

      case PureKafkaConsumer(brokers, consGroup, offset, _) =>
        Either.catchNonFatal(scheduler.scheduleWithFixedDelay(0.seconds, 3.seconds) {
          sinkWithPureKafka(in, out, storage, brokers, offset, consGroup, ext, fs)
        })

      case SparkKafkaPluginStreamsConsumer(brokers, offset, _) =>
        Either.catchNonFatal {
          val thread = new Thread {
            override def run(): Unit = {
              sinkViaSparkKafkaStreamsPlugin(
                DataFrameUtils.sparkSession,
                in,
                out,
                storage,
                brokers,
                offset,
                ext
              )
            }
          }
          thread.start()
        }

      case SparkKafkaPluginConsumer(brokers, offset, _) =>
        // one-shot job
        Either.catchNonFatal(
          sinkViaSparkKafkaPlugin(
            DataFrameUtils.sparkSession,
            in,
            out,
            storage,
            brokers,
            offset,
            ext,
            fs
          )
        )
      case _ =>
        throw new RuntimeException(
          s"This mode is not supported while consuming data. The provided input kafka mode is : '$kafkaMode'"
        )
    }
  }

  /**
    * Computes the generated output files extension based on the output DataType
    *
    * @param dataType The output files DataType
    * @return The output extension
    */
  @deprecated("To be deleted. To set JSON as default.")
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
    * Sinks topic content into files using a [[io.oss.data.highway.models.SparkKafkaPluginConsumer]]
    *
    * @param session The Spark session
    * @param in The input kafka topic
    * @param out The output folder
    * @param storage The output file system storage
    * @param brokerUrls The kafka brokers urls
    * @param offset The kafka consumer offset
    * @param extension The output files extension
    * @return Unit
    */
  private def sinkViaSparkKafkaPlugin(
      session: SparkSession,
      in: String,
      out: String,
      storage: Storage,
      brokerUrls: String,
      offset: Offset,
      extension: String,
      fs: FileSystem
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
      s"Starting to sink '$extension' data provided by the input topic '$in' in the output folder pattern" +
        s" '$out/spark-kafka-plugin-*****$extension'"
    )
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
        storage match {
          case Local =>
            FilesUtils.createFile(
              out,
              s"spark-kafka-plugin-${UUID.randomUUID()}-${System.currentTimeMillis()}.$extension",
              data.toString()
            )
          case HDFS =>
            HdfsUtils.save(
              fs,
              s"$out/spark-kafka-plugin-${UUID.randomUUID()}-${System.currentTimeMillis()}.$extension",
              data.toString()
            )
        }
      )
  }

  /**
    * Sinks topic content into files using a [[io.oss.data.highway.models.SparkKafkaPluginStreamsConsumer]]
    *
    * @param session The Spark session
    * @param in The input kafka topic
    * @param out The output folder
    * @param storage The output file system storage
    * @param brokerUrls The kafka brokers urls
    * @param offset The kafka consumer offset
    * @param extension The output files extension
    * @return Unit
    */
  private def sinkViaSparkKafkaStreamsPlugin(
      session: SparkSession,
      in: String,
      out: String,
      storage: Storage,
      brokerUrls: String,
      offset: Offset,
      extension: String
  ): Unit = {
    import session.implicits._
    logger.info(
      s"Starting to sink '$extension' data provided by the input topic '$in' in the output folder pattern " +
        s"'$out/spark-kafka-streaming-plugin-*****$extension'"
    )
    storage match {
      case Local =>
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
          .option("path", s"$out/spark-kafka-streaming-plugin-${System.currentTimeMillis()}")
          .option(
            "checkpointLocation",
            s"/tmp/checkpoint/${UUID.randomUUID()}-${System.currentTimeMillis()}"
          )
          .start()
          .awaitTermination()
      case HDFS =>
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
          .option("path", s"$out/spark-kafka-streaming-plugin-${System.currentTimeMillis()}")
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
    * @param in The input kafka topic
    * @param out The output folder
    * @param storage The output file system storage
    * @param brokerUrls The kafka brokers urls
    * @param offset The kafka consumer offset
    * @param extension The output files extension
    * @param streamAppId The identifier of the streaming application
    * @return a Unit, otherwise an Error
    */
  private def sinkWithPureKafkaStreams(
      in: String,
      out: String,
      storage: Storage,
      brokerUrls: String,
      offset: Offset,
      extension: String,
      streamAppId: String,
      fs: FileSystem
  ): Either[Throwable, Unit] = {
    Try {
      val kafkaStreamEntity =
        KafkaTopicConsumer.consumeWithStream(streamAppId, in, offset, brokerUrls)
      kafkaStreamEntity.dataKStream.mapValues(data => {
        val uuid: String = s"${UUID.randomUUID()}-${System.currentTimeMillis()}"
        storage match {
          case Local =>
            FilesUtils.createFile(out, s"kafka-streams-$uuid.$extension", data)
          case HDFS =>
            HdfsUtils.save(fs, s"$out/kafka-streams-$uuid.$extension", data)
        }
        logger.info(
          s"Successfully sinking '$extension' data provided by the input topic '$in' in the output folder pattern " +
            s"'$out/kafka-streams-*****$extension'"
        )
      })
      val streams = new KafkaStreams(kafkaStreamEntity.builder.build(), kafkaStreamEntity.props)
      streams.start()
    }.toEither
  }

  /**
    * Sinks topic content into files using a [[io.oss.data.highway.models.PureKafkaConsumer]]
    *
    * @param in The input kafka topic
    * @param out The output folder
    * @param storage The output file system storage
    * @param brokerUrls The kafka brokers urls
    * @param offset The kafka consumer offset
    * @param consumerGroup The consumer group
    * @param extension The output files extension
    * @param fs The provided File System
    * @return a Unit, otherwise an Error
    */
  private def sinkWithPureKafka(
      in: String,
      out: String,
      storage: Storage,
      brokerUrls: String,
      offset: Offset,
      consumerGroup: String,
      extension: String,
      fs: FileSystem
  ): Either[KafkaError, Unit] = {
    KafkaTopicConsumer
      .consume(in, brokerUrls, offset, consumerGroup)
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
                .createFile(out, s"simple-consumer-$uuid.$extension", data.value())
            case HDFS =>
              HdfsUtils.save(fs, s"$out/simple-consumer-$uuid.$extension", data.value())
          }
          logger.info(
            s"Successfully sinking '$extension' data provided by the input topic '$in' in the output folder pattern " +
              s"'$out/simple-consumer-*****$extension'"
          )
        }
        consumed.close() // Close it to rejoin again while rescheduling
      })
  }
}
