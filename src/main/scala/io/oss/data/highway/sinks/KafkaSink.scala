package io.oss.data.highway.sinks

import java.time.Duration
import org.apache.kafka.clients.producer._

import java.util.{Properties, UUID}
import io.oss.data.highway.models.{
  HDFS,
  JSON,
  KafkaMode,
  Local,
  Offset,
  PureKafkaProducer,
  PureKafkaStreamsProducer,
  SparkKafkaPluginProducer,
  SparkKafkaPluginStreamsProducer,
  Storage
}
import io.oss.data.highway.utils.{DataFrameUtils, FilesUtils, HdfsUtils, KafkaUtils}
import org.apache.kafka.common.serialization.{Serdes, StringSerializer}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.log4j.Logger
import cats.implicits._
import io.oss.data.highway.models.DataHighwayError.KafkaError
import monix.execution.Scheduler.{global => scheduler}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.concurrent.duration._
import java.io.File
import scala.sys.ShutdownHookThread

object KafkaSink extends HdfsUtils {

  val logger: Logger            = Logger.getLogger(KafkaSink.getClass.getName)
  val generated: String         = s"${UUID.randomUUID()}-${System.currentTimeMillis().toString}"
  val intermediateTopic: String = s"intermediate-topic-$generated"
  val checkpointFolder: String  = s"/tmp/data-highway/checkpoint-$generated"

  /**
    * Publishes message to Kafka topic
    *
    * @param input The input topic or the input path that contains json data to be send
    * @param topic The output topic
    * @param storage The input file system storage : Local or HDFS
    * @param kafkaMode The Kafka launch mode
    * @return Any, otherwise an Error
    */
  def publishToTopic(
      input: String,
      topic: String,
      storage: Storage,
      kafkaMode: KafkaMode
  ): Either[Throwable, Any] = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaMode.brokers)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    val prod = new KafkaProducer[String, String](props)
    KafkaUtils.verifyTopicExistence(topic, kafkaMode.brokers, enableTopicCreation = true)
    kafkaMode match {

      case PureKafkaStreamsProducer(brokers, streamAppId, offset, _) =>
        runStream(streamAppId, input, brokers, topic, offset)

      case PureKafkaProducer(_, _) =>
        Either.catchNonFatal {
          scheduler.scheduleWithFixedDelay(0.seconds, 3.seconds) {
            publishPathContent(input, topic, storage, prod, fs)
            storage match {
              case Local =>
                FilesUtils.cleanup(input)
              case HDFS =>
                HdfsUtils.cleanup(fs, input)
            }
          }
        }

      case SparkKafkaPluginStreamsProducer(brokers, offset, _) =>
        Either.catchNonFatal {
          val thread = new Thread {
            override def run(): Unit = {
              publishWithSparkKafkaStreamsPlugin(
                input,
                prod,
                brokers,
                topic,
                checkpointFolder,
                offset
              )
            }
          }
          thread.start()
        }

      case SparkKafkaPluginProducer(brokers, _) =>
        Either.catchNonFatal(scheduler.scheduleWithFixedDelay(0.seconds, 3.seconds) {
          publishWithSparkKafkaPlugin(input, storage, brokers, topic, fs)
          storage match {
            case Local =>
              FilesUtils.cleanup(input)
            case HDFS =>
              HdfsUtils.cleanup(fs, input)
          }
        })
      case _ =>
        throw new RuntimeException(
          s"This mode is not supported while producing data. The provided input kafka mode is '$kafkaMode'."
        )
    }
  }

  /**
    * Publishes message via [[io.oss.data.highway.models.SparkKafkaPluginProducer]]
    *
    * @param jsonPath The path that contains json data to be send
    * @param storage The input file system storage : Local or HDFS
    * @param brokers The kafka brokers urls
    * @param topic The output topic
    * @param fs The provided File System
    * @return Unit, otherwise an Error
    */
  private def publishWithSparkKafkaPlugin(
      jsonPath: String,
      storage: Storage,
      brokers: String,
      topic: String,
      fs: FileSystem
  ): Either[Throwable, Unit] = {
    import org.apache.spark.sql.functions.{to_json, struct}
    logger.info(s"Sending data through Spark Kafka Plugin to '$topic'.")
    storage match {
      case HDFS =>
        val basePath = new Path(jsonPath).getParent
        HdfsUtils
          .listFolders(fs, jsonPath)
          .flatMap(paths => HdfsUtils.verifyNotEmpty(fs, paths))
          .map(paths => {
            paths.map(path => {
              DataFrameUtils
                .loadDataFrame(path, JSON)
                .map(df => {
                  df.select(to_json(struct("*")).as("value"))
                    .write
                    .format("kafka")
                    .option("kafka.bootstrap.servers", brokers)
                    .option("topic", topic)
                    .save()
                })
              HdfsUtils.movePathContent(fs, path, basePath.toString)
            })
          })

      case Local =>
        val basePath = new File(jsonPath).getParent
        FilesUtils
          .listNonEmptyFoldersRecursively(jsonPath)
          .map(paths => {
            paths
              .filterNot(path => new File(path).listFiles.filter(_.isFile).toList.isEmpty)
              .map(path => {
                DataFrameUtils
                  .loadDataFrame(path, JSON)
                  .map(df => {
                    df.select(to_json(struct("*")).as("value"))
                      .write
                      .format("kafka")
                      .option("kafka.bootstrap.servers", brokers)
                      .option("topic", topic)
                      .save()
                  })
                FilesUtils.movePathContent(
                  new File(path).getAbsolutePath,
                  s"$basePath/processed"
                )
              })
          })
    }
  }

  /**
    * Publishes a message via Spark [[io.oss.data.highway.models.SparkKafkaPluginStreamsProducer]]
    *
    * @param input The path that contains json data to be send
    * @param producer The Kafka Producer
    * @param bootstrapServers The kafka brokers urls
    * @param outputTopic The output topic
    * @param checkpointFolder The checkpoint folder
    * @param offset The Kafka offset from where the message consumption will begin
    * @return Unit, otherwise an Error
    */
  private def publishWithSparkKafkaStreamsPlugin(
      input: String,
      producer: KafkaProducer[String, String],
      bootstrapServers: String,
      outputTopic: String,
      checkpointFolder: String,
      offset: Offset
  ): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      DataFrameUtils.sparkSession.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("startingOffsets", offset.value)
        .option("subscribe", input)
        .load()
        .selectExpr("CAST(value AS STRING)")
        .writeStream
        .format("kafka") // console
        .option("truncate", false)
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("checkpointLocation", checkpointFolder)
        .option("topic", outputTopic)
        .start()
        .awaitTermination()
    }
  }

  /**
    * Runs Kafka stream via [[io.oss.data.highway.models.PureKafkaStreamsProducer]]
    *
    * @param streamAppId The Kafka stream application id
    * @param intermediateTopic The Kafka intermediate topic
    * @param bootstrapServers The kafka brokers urls
    * @param streamsOutputTopic The Kafka output topic
    * @param offset The Kafka offset from where the message consumption will begin
    * @return ShutdownHookThread, otherwise an Error
    */
  private def runStream(
      streamAppId: String,
      intermediateTopic: String,
      bootstrapServers: String,
      streamsOutputTopic: String,
      offset: Offset
  ): Either[Throwable, ShutdownHookThread] = {
    Either.catchNonFatal {
      import org.apache.kafka.streams.scala.ImplicitConversions._
      import org.apache.kafka.streams.scala.Serdes._

      val props = new Properties
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, streamAppId)
      props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset.value)

      val builder = new StreamsBuilder

      val dataKStream = builder.stream[String, String](intermediateTopic)
      dataKStream.to(streamsOutputTopic)

      val streams = new KafkaStreams(builder.build(), props)
      logger.info(
        s"Sending data through Kafka streams to '$intermediateTopic' topic - Sent data: '$streamsOutputTopic'."
      )

      streams.start()

      sys.ShutdownHookThread {
        streams.close(Duration.ofSeconds(10))
      }
    }
  }

  /**
    * Publishes json files located under a provided path to Kafka topic.
    *
    * @param jsonPath The path that may be a file or a folder containing multiples sub-folders and files.
    * @param topic The output Kafka topic
    * @param storage The input file system storage : Local or HDFS
    * @param producer The Kafka producer
    * @param fs The provided File System
    * @return Any, otherwise an Error
    */
  private def publishPathContent(
      jsonPath: String,
      topic: String,
      storage: Storage,
      producer: KafkaProducer[String, String],
      fs: FileSystem
  ): Either[KafkaError, Any] = {
    Either.catchNonFatal {
      storage match {
        case HDFS =>
          val basePath = new Path(jsonPath).getParent
          HdfsUtils
            .listFilesRecursively(fs, jsonPath)
            .foreach(file => {
              publishFileContent(file, basePath.toString, storage, topic, producer, fs)
            })

        case Local =>
          val basePath = new File(jsonPath).getParent
          FilesUtils
            .listFilesRecursively(new File(jsonPath), JSON.extension)
            .foreach(file => {
              publishFileContent(file.getAbsolutePath, basePath, storage, topic, producer, fs)
            })
      }
    }.leftMap(thr => KafkaError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Publishes the content of the json file via [[io.oss.data.highway.models.PureKafkaProducer]]
    *
    * @param jsonPath a json file or a folder or folders that contain json files
    * @param basePath The base path for input, output and processed folders
    * @param storage The input file system storage : Local or HDFS
    * @param topic The destination topic
    * @param producer The Kafka producer
    * @param fs The provided File System
    * @return List of String, otherwise a Throwable
    */
  private def publishFileContent(
      jsonPath: String,
      basePath: String,
      storage: Storage,
      topic: String,
      producer: KafkaProducer[String, String],
      fs: FileSystem
  ): Either[KafkaError, List[String]] = {
    logger.info(s"Sending data of '$jsonPath'")
    Either.catchNonFatal {
      storage match {
        case Local =>
          FilesUtils
            .getLines(jsonPath)
            .foreach(line => {
              val uuid = UUID.randomUUID().toString
              val data =
                new ProducerRecord[String, String](topic, uuid, line)
              producer.send(data)
              logger.info(s"Topic: '$topic' - Sent data: '$line'")
            })
          FilesUtils.movePathContent(
            jsonPath,
            s"$basePath/processed/${new File(jsonPath).getParentFile.getName}"
          )

        case HDFS =>
          HdfsUtils
            .getJsonLines(fs, jsonPath)
            .foreach(line => {
              val uuid = UUID.randomUUID().toString
              val data =
                new ProducerRecord[String, String](topic, uuid, line)
              producer.send(data)
              logger.info(s"Topic: '$topic' - Sent data: '$line'")
            })
          HdfsUtils.movePathContent(fs, jsonPath, basePath)
      }
    }.flatten.leftMap(thr => KafkaError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }
}
