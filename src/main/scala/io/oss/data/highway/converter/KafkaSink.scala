package io.oss.data.highway.converter

import java.time.Duration
import org.apache.kafka.clients.producer._

import java.util.{Properties, UUID}
import io.oss.data.highway.model.{
  JSON,
  KafkaMode,
  Offset,
  PureKafkaProducer,
  PureKafkaStreamsProducer,
  SparkKafkaPluginProducer,
  SparkKafkaPluginStreamsProducer
}
import io.oss.data.highway.utils.{DataFrameUtils, FilesUtils, KafkaUtils}
import org.apache.kafka.common.serialization.{Serdes, StringSerializer}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import scala.io.Source
import org.apache.log4j.Logger
import cats.implicits._
import io.oss.data.highway.configuration.SparkConfigs
import io.oss.data.highway.model.DataHighwayError.KafkaError
import monix.execution.Scheduler.{global => scheduler}
import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.concurrent.duration._
import java.io.File
import scala.sys.ShutdownHookThread

class KafkaSink {

  val logger: Logger = Logger.getLogger(classOf[KafkaSink].getName)
  val generated =
    s"${UUID.randomUUID()}-${System.currentTimeMillis().toString}"
  val intermediateTopic: String =
    s"intermediate-topic-$generated"
  val checkpointFolder: String =
    s"/tmp/data-highway/checkpoint-$generated"

  /**
    * Publishes message to Kafka topic
    * @param input The input topic or the input path that contains json data to be send
    * @param topic The output topic
    * @param brokers The kafka brokers urls
    * @param kafkaMode The Kafka launch mode : SimpleProducer, KafkaStreaming or SparkKafkaPlugin
    * @param sparkConfig The Spark configuration
    * @return Any, otherwise an Error
    */
  def publishToTopic(input: String,
                     topic: String,
                     brokers: String,
                     kafkaMode: KafkaMode,
                     sparkConfig: SparkConfigs): Either[Throwable, Any] = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    val prod = new KafkaProducer[String, String](props)
    KafkaUtils.verifyTopicExistence(topic, brokers, enableTopicCreation = true)
    kafkaMode match {
      case PureKafkaStreamsProducer(streamAppId, offset) =>
        runStream(streamAppId, input, brokers, topic, offset)

      case PureKafkaProducer =>
        Either.catchNonFatal {
          scheduler.scheduleWithFixedDelay(0.seconds, 3.seconds) {
            publishPathContent(input, topic, prod)
            FilesUtils.deleteFolder(input)
          }
        }

      case SparkKafkaPluginStreamsProducer(offset) =>
        Either.catchNonFatal {
          val thread = new Thread {
            override def run() {
              publishWithSparkKafkaStreamsPlugin(input,
                                                 prod,
                                                 brokers,
                                                 topic,
                                                 checkpointFolder,
                                                 sparkConfig,
                                                 offset)
            }
          }
          thread.start()
        }
      case SparkKafkaPluginProducer =>
        Either.catchNonFatal(
          scheduler.scheduleWithFixedDelay(0.seconds, 3.seconds) {
            publishWithSparkKafkaPlugin(input, brokers, topic, sparkConfig)
            FilesUtils.deleteFolder(input)
          })
      case _ =>
        throw new RuntimeException(
          s"This mode is not supported while producing data. The supported Kafka Consume Mode are : '${PureKafkaProducer.getClass.getName}' and '${SparkKafkaPluginProducer.getClass.getName}'.")
    }
  }

  /**
    * Publishes message via Spark Kafka-Plugin
    * @param jsonPath The path that contains json data to be send
    * @param brokers The kafka brokers urls
    * @param topic The output topic
    * @param sparkConf The spark configuration
    * @return Unit, otherwise an Error
    */
  private def publishWithSparkKafkaPlugin(
      jsonPath: String,
      brokers: String,
      topic: String,
      sparkConf: SparkConfigs): Either[Throwable, Unit] = {
    import org.apache.spark.sql.functions.{to_json, struct}
    logger.info(s"Sending data through Spark Kafka Plugin to '$topic'.")
    val basePath = new File(jsonPath).getParent
    FilesUtils
      .listFoldersRecursively(jsonPath)
      .map(paths => {
        paths
          .filterNot(path =>
            new File(path).listFiles.filter(_.isFile).toList.isEmpty)
          .map(path => {
            DataFrameUtils(sparkConf)
              .loadDataFrame(path, JSON)
              .map(df => {
                df.select(to_json(struct("*")).as("value"))
                  .write
                  .format("kafka")
                  .option("kafka.bootstrap.servers", brokers)
                  .option("topic", topic)
                  .save()
              })
            FilesUtils.movePathContent(new File(path).getAbsolutePath, basePath)
          })
      })
  }

  /**
    * Publishes a message via Spark Kafka-Stream-Plugin
    * @param input The path that contains json data to be send
    * @param producer The Kafka Producer
    * @param bootstrapServers The kafka brokers urls
    * @param outputTopic The output topic
    * @param checkpointFolder The checkpoint folder
    * @param sparkConfig The Spark configuration
    * @return Unit, otherwise an Error
    */
  private def publishWithSparkKafkaStreamsPlugin(
      input: String,
      producer: KafkaProducer[String, String],
      bootstrapServers: String,
      outputTopic: String,
      checkpointFolder: String,
      sparkConfig: SparkConfigs,
      offset: Offset): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      DataFrameUtils(sparkConfig).sparkSession.readStream
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
    * Runs Kafka stream
    * @param streamAppId The Kafka stream application id
    * @param intermediateTopic The Kafka intermediate topic
    * @param bootstrapServers The kafka brokers urls
    * @param streamsOutputTopic The Kafka output topic
    * @return ShutdownHookThread, otherwise an Error
    */
  private def runStream(
      streamAppId: String,
      intermediateTopic: String,
      bootstrapServers: String,
      streamsOutputTopic: String,
      offset: Offset): Either[Throwable, ShutdownHookThread] = {
    Either.catchNonFatal {
      import org.apache.kafka.streams.scala.ImplicitConversions._
      import org.apache.kafka.streams.scala.Serdes._

      val props = new Properties
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, streamAppId)
      props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass)
      props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass)
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset.value)

      val builder = new StreamsBuilder

      val dataKStream = builder.stream[String, String](intermediateTopic)
      dataKStream.to(streamsOutputTopic)

      val streams = new KafkaStreams(builder.build(), props)
      logger.info(
        s"Sending data through Kafka streams to '$intermediateTopic' topic - Sent data: '$streamsOutputTopic'.")

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
    * @param producer The Kafka producer
    * @return Any, otherwise an Error
    */
  private def publishPathContent(
      jsonPath: String,
      topic: String,
      producer: KafkaProducer[String, String]): Either[KafkaError, Any] = {
    val basePath = new File(jsonPath).getParent
    Either
      .catchNonFatal {
        if (new File(jsonPath).isFile) {
          publishFileContent(new File(jsonPath), basePath, topic, producer)
        } else {
          FilesUtils
            .listFilesRecursively(new File(jsonPath), Seq(JSON.extension))
            .foreach(file => {
              publishFileContent(file, basePath, topic, producer)
            })
        }
      }
      .leftMap(thr =>
        KafkaError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Publishes the content of the json file
    * @param jsonPath a json file or a folder or folders that contain json files
    * @param topic The destination topic
    * @param producer The Kafka producer
    */
  private def publishFileContent(jsonPath: File,
                                 basePath: String,
                                 topic: String,
                                 producer: KafkaProducer[String, String]) = {
    logger.info(s"Sending data of '${jsonPath.getAbsolutePath}'")
    Either
      .catchNonFatal {
        for (line <- getJsonLines(jsonPath.getAbsolutePath)) {
          val uuid = UUID.randomUUID().toString
          val data =
            new ProducerRecord[String, String](topic, uuid, line)
          producer.send(data)
          logger.info(s"Topic: '$topic' - Sent data: '$line'")
          FilesUtils.movePathContent(
            new File(jsonPath.getAbsolutePath).getParent,
            basePath)
        }
      }
      .leftMap(thr =>
        KafkaError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Get lines from json file
    *
    * @param jsonPath The path that contains json data to be send
    * @return an Iterator of String
    */
  private def getJsonLines(jsonPath: String): Iterator[String] = {
    val jsonFile = Source.fromFile(jsonPath)
    jsonFile.getLines
  }
}
