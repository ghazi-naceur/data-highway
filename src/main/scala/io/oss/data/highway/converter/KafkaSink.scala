package io.oss.data.highway.converter

import java.time.Duration
import org.apache.kafka.clients.producer._

import java.util.{Properties, UUID}
import io.oss.data.highway.model.{
  JSON,
  KafkaMode,
  Latest,
  Offset,
  PureKafkaProducer,
  SparkKafkaProducerPlugin
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
    * Sends message to Kafka topic
    * @param jsonPath The path that contains json data to be send
    * @param topic The output topic
    * @param bootstrapServers The kafka brokers urls
    * @param kafkaMode The Kafka launch mode : SimpleProducer, KafkaStreaming or SparkKafkaPlugin
    * @param sparkConfig The Spark configuration
    * @return Any, otherwise an Error
    */
  def sendToTopic(jsonPath: String,
                  topic: String,
                  bootstrapServers: String,
                  kafkaMode: KafkaMode,
                  sparkConfig: SparkConfigs): Either[Throwable, Any] = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](props)
    KafkaUtils.verifyTopicExistence(topic,
                                    bootstrapServers,
                                    enableTopicCreation = true)
    kafkaMode match {
      case PureKafkaProducer(useStream, streamAppId) =>
        if (useStream) {
          KafkaUtils.verifyTopicExistence(intermediateTopic,
                                          bootstrapServers,
                                          enableTopicCreation = true)
          send(jsonPath, intermediateTopic, producer)
          streamAppId match {
            case Some(id) =>
              runStream(id, intermediateTopic, bootstrapServers, topic)
            case None =>
              throw new RuntimeException(
                "At this stage, 'stream-app-id' is set and Streaming producer is activated. 'stream-app-id' cannot be set to None.")
          }
        } else {
          send(jsonPath, topic, producer)
        }
      case SparkKafkaProducerPlugin(useStream) =>
        if (useStream) {
          sendUsingStreamSparkKafkaPlugin(jsonPath,
                                          producer,
                                          bootstrapServers,
                                          topic,
                                          intermediateTopic,
                                          checkpointFolder,
                                          sparkConfig)
        } else {
          sendUsingSparkKafkaPlugin(jsonPath,
                                    bootstrapServers,
                                    topic,
                                    sparkConfig)
        }
      case _ =>
        throw new RuntimeException(
          s"This mode is not supported while producing data. The supported Kafka Consume Mode are : '${PureKafkaProducer.getClass.getName}' and '${SparkKafkaProducerPlugin.getClass.getName}'.")
    }
  }

  /**
    * Sends message via Spark Kafka-Plugin
    * @param jsonPath The path that contains json data to be send
    * @param bootstrapServers The kafka brokers urls
    * @param topic The output topic
    * @param sparkConfig The spark configuration
    * @return Unit, otherwise an Error
    */
  private def sendUsingSparkKafkaPlugin(
      jsonPath: String,
      bootstrapServers: String,
      topic: String,
      sparkConfig: SparkConfigs): Either[Throwable, Unit] = {
    import org.apache.spark.sql.functions.{col, to_json, struct}
    DataFrameUtils(sparkConfig)
      .loadDataFrame(jsonPath, JSON)
      .map(df => {
        df.select(to_json(struct("*")).as("value"))
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", bootstrapServers)
          .option("topic", topic)
          .save()
        logger.info(s"Sending data through Spark Kafka Plugin to '$topic'.")
      })
  }

  /**
    * Sends message via Spark Kafka-Stream-Plugin
    * @param jsonPath The path that contains json data to be send
    * @param producer The Kafka Producer
    * @param bootstrapServers The kafka brokers urls
    * @param outputTopic The output topic
    * @param intermediateTopic The intermediate kafka topic
    * @param checkpointFolder The checkpoint folder
    * @param sparkConfig The Spark configuration
    * @param offset The Kafka consumer offset
    * @return Unit, otherwise an Error
    */
  private def sendUsingStreamSparkKafkaPlugin(
      jsonPath: String,
      producer: KafkaProducer[String, String],
      bootstrapServers: String,
      outputTopic: String,
      intermediateTopic: String,
      checkpointFolder: String,
      sparkConfig: SparkConfigs,
      offset: Offset = Latest): Either[Throwable, Unit] = {
    //TODO offset has a default value that could be externalized
    Either.catchNonFatal {
      KafkaUtils.verifyTopicExistence(intermediateTopic,
                                      bootstrapServers,
                                      enableTopicCreation = true)
      send(jsonPath, intermediateTopic, producer)

      DataFrameUtils(sparkConfig).sparkSession.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("startingOffsets", "earliest")
        .option("subscribe", intermediateTopic)
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
      streamsOutputTopic: String): Either[Throwable, ShutdownHookThread] = {
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
    * Sends message to Kafka topic
    *
    * @param jsonPath The path that contains json data to be send
    * @param topic The output Kafka topic
    * @param producer The Kafka producer
    * @return Any, otherwise an Error
    */
  private def send(
      jsonPath: String,
      topic: String,
      producer: KafkaProducer[String, String]): Either[KafkaError, Any] = {
    Either
      .catchNonFatal {
        if (new File(jsonPath).isFile) {
          publishFileContent(new File(jsonPath), topic, producer)
        } else {
          FilesUtils
            .listFilesRecursively(new File(jsonPath),
                                  Seq(JSON.extension.substring(1)))
            .foreach(file => {
              publishFileContent(file, topic, producer)
            })
        }
        producer.close()
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
