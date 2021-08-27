package io.oss.data.highway.engine

import java.time.Duration
import java.util.UUID
import io.oss.data.highway.models.{
  Cassandra,
  Earliest,
  Elasticsearch,
  File,
  HDFS,
  JSON,
  Kafka,
  KafkaConsumer,
  Local,
  Offset,
  Output,
  PureKafkaConsumer,
  PureKafkaStreamsConsumer,
  SparkKafkaPluginConsumer,
  SparkKafkaPluginStreamsConsumer,
  Storage
}
import io.oss.data.highway.utils.{FilesUtils, HdfsUtils, KafkaTopicConsumer, KafkaUtils}
import org.apache.kafka.streams.KafkaStreams
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import monix.execution.Scheduler.{global => scheduler}
import cats.implicits._
import io.oss.data.highway.models.DataHighwayError.DataHighwayFileError
import io.oss.data.highway.utils.DataFrameUtils.sparkSession
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.functions.{struct, to_json}

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
      output: Output,
      saveMode: SaveMode,
      storage: Option[Storage]
  ): Either[Throwable, Unit] = {
    val tempoPathSuffix =
      s"/tmp/data-highway/kafka-sampler/kafka-plugin/${System.currentTimeMillis().toString}/"
    val temporaryPath = tempoPathSuffix + UUID.randomUUID().toString
    val consumer      = input.kafkaMode.asInstanceOf[Option[KafkaConsumer]]
    (storage, consumer) match {
      case (Some(filesystem), Some(km)) =>
        KafkaUtils.verifyTopicExistence(input.topic, km.brokers, enableTopicCreation = false)
        km match {
          case PureKafkaConsumer(brokers, consGroup, offset) =>
            // Continuous job - to be triggered once
            Either.catchNonFatal(scheduler.scheduleWithFixedDelay(0.seconds, 5.seconds) {
              sinkWithPureKafkaConsumer(
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
            // Continuous job - to be triggered once
            sinkWithPureKafkaStreamsConsumer(
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
            // Batch/One-shot job - to be triggered everytime
            Either.catchNonFatal {
              sinkWithSparkKafkaConnector(
                sparkSession,
                input,
                output,
                temporaryPath,
                tempoPathSuffix,
                filesystem,
                saveMode,
                brokers,
                offset
              )
            }
//          case SparkKafkaPluginStreamsConsumer(brokers, offset) =>
//            // only json data type support
//            Either.catchNonFatal {
//              val thread = new Thread(() => {
//                sinkWithSparkKafkaStreamsConnector(
//                  sparkSession,
//                  input.topic,
//                  output.path,
//                  filesystem,
//                  brokers,
//                  offset
//                )
//              })
//              thread.start()
//            }
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
  private[engine] def sinkWithPureKafkaConsumer(
      inputTopic: String,
      temporaryPath: String,
      tempoPathSuffix: String,
      output: Output,
      storage: Storage,
      saveMode: SaveMode,
      brokerUrls: String,
      offset: Offset,
      consumerGroup: String,
      fs: FileSystem
  ): Either[Throwable, Unit] = {
    val tempoBasePath = new java.io.File(temporaryPath).getParent
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
        consumed.close()
      })
    sendDataToVariousOutputs(
      temporaryPath,
      tempoPathSuffix,
      output,
      storage,
      saveMode,
      tempoBasePath
    )
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
  private[engine] def sinkWithPureKafkaStreamsConsumer(
      inputTopic: String,
      temporaryPath: String,
      tempoPathSuffix: String,
      output: Output,
      storage: Storage,
      saveMode: SaveMode,
      brokerUrls: String,
      offset: Offset,
      streamAppId: String,
      fs: FileSystem
  ): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      val tempoBasePath = new java.io.File(temporaryPath).getParent
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
        sendDataToVariousOutputs(
          temporaryPath,
          tempoPathSuffix,
          output,
          storage,
          saveMode,
          tempoBasePath
        )
      })
      val streams = new KafkaStreams(kafkaStreamEntity.builder.build(), kafkaStreamEntity.props)
      streams.start()
    }
  }

  def sinkWithSparkKafkaConnector(
      session: SparkSession,
      kafka: Kafka,
      output: Output,
      temporaryPath: String,
      tempoPathSuffix: String,
      storage: Storage,
      saveMode: SaveMode,
      brokerUrls: String,
      offset: Offset
  ): Either[Throwable, Unit] = {
    val tempoBasePath = new java.io.File(temporaryPath).getParent
    import session.implicits._
    var mutableOffset = offset
    if (mutableOffset != Earliest) {
      logger.warn(
        s"Starting offset can't be ${mutableOffset.value} for batch queries on Kafka. So, we'll set it at 'Earliest'."
      )
      mutableOffset = Earliest
    }
    logger.info(
      s"Starting to sink '${JSON.extension}' data provided by the input topic '${kafka.topic}' in the output folder pattern" +
        s" '$temporaryPath/spark-kafka-plugin-*****${JSON.extension}'"
    )
    val frame = session.read
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerUrls)
      .option("startingOffsets", mutableOffset.value)
      .option("subscribe", kafka.topic)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .select(to_json(struct("value")))
      .toJavaRDD

    frame
      .collect()
      .asScala
      .toList
      .traverse(data => {
        val line = data.toString.substring(11, data.toString().length - 3).replace("\\", "")
        storage match {
          case Local =>
            FilesUtils.createFile(
              temporaryPath,
              s"spark-kafka-plugin-${UUID.randomUUID()}-${System.currentTimeMillis()}.${JSON.extension}",
              line
            )
          case HDFS =>
            HdfsUtils.save(
              fs,
              s"$temporaryPath/spark-kafka-plugin-${UUID.randomUUID()}-${System
                .currentTimeMillis()}.${JSON.extension}",
              line
            )
        }
      })
    sendDataToVariousOutputs(
      temporaryPath,
      tempoPathSuffix,
      output,
      storage,
      saveMode,
      tempoBasePath
    )
  }

//  /**
//    * Sinks topic content into files using a [[io.oss.data.highway.models.SparkKafkaPluginStreamsConsumer]]
//    *
//    * @param session The Spark session
//    * @param inputTopic The input kafka topic
//    * @param storage The output file system storage
//    * @param brokerUrls The kafka brokers urls
//    * @param offset The kafka consumer offset
//    */
//  private[engine] def sinkWithSparkKafkaStreamsConnector(
//      session: SparkSession,
//      inputTopic: String,
//      outputPath: String,
//      storage: Storage,
//      brokerUrls: String,
//      offset: Offset
//  ): Unit = {
//    import session.implicits._
//    logger.info(
//      s"Starting to sink '${JSON.extension}' data provided by the input topic '$inputTopic' in the output folder pattern " +
//        s"'$outputPath/spark-kafka-streaming-plugin-*****${JSON.extension}'"
//    )
//    val stream = session.readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", brokerUrls)
//      .option("startingOffsets", offset.value)
//      .option("subscribe", inputTopic)
//      .load()
//      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//      .as[(String, String)]
//      .select("value")
//      .writeStream
//      .format(JSON.extension)
//      .option("path", s"$outputPath/spark-kafka-streaming-plugin-${System.currentTimeMillis()}")
//    storage match {
//      case Local =>
//        stream
//          .option(
//            "checkpointLocation",
//            s"/tmp/checkpoint/${UUID.randomUUID()}-${System.currentTimeMillis()}"
//          )
//          .start()
//          .awaitTermination()
//      case HDFS =>
//        stream
//          .option(
//            "checkpointLocation",
//            s"${hadoopConf.host}/tmp/checkpoint/${UUID.randomUUID()}-${System.currentTimeMillis()}"
//          )
//          .start()
//          .awaitTermination()
//    }
//  }

  private def sendDataToVariousOutputs(
      temporaryPath: String,
      tempoPathSuffix: String,
      output: Output,
      storage: Storage,
      saveMode: SaveMode,
      tempoBasePath: String
  ): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      output match {
        case file @ File(_, _) =>
          convertUsingBasicSink(temporaryPath, tempoPathSuffix, file, storage, saveMode)
        case cassandra @ Cassandra(_, _) =>
          CassandraSink.insertRows(
            File(JSON, temporaryPath),
            cassandra,
            tempoBasePath,
            saveMode
          )
        case elasticsearch @ Elasticsearch(_, _, _) =>
          ElasticSink
            .insertDocuments(
              File(JSON, temporaryPath),
              elasticsearch,
              tempoBasePath,
              storage
            )
        case Kafka(_, _) =>
//          KafkaSink.mirrorTopic(Kafka(inputTopic, None), kafka)
          new RuntimeException("Already taken care of by kafka-to-kafka routes")
      }
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
