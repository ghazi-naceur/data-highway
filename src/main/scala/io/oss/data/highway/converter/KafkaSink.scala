package io.oss.data.highway.converter

import java.time.Duration

import org.apache.kafka.clients.producer._
import java.util.{Properties, UUID}

import io.oss.data.highway.model.{
  JSON,
  KafkaMode,
  KafkaStreaming,
  Latest,
  Offset,
  SimpleProducer,
  SparkKafkaProducerPlugin
}
import io.oss.data.highway.utils.{DataFrameUtils, KafkaUtils}
import org.apache.kafka.common.serialization.{Serdes, StringSerializer}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import scala.io.Source
import scala.util.Try
import org.apache.log4j.Logger
import cats.syntax.either._
import io.oss.data.highway.configuration.{KafkaConfigs, SparkConfigs}
import io.oss.data.highway.model.DataHighwayError.KafkaError
import org.apache.spark.sql.functions.lit

import scala.sys.ShutdownHookThread

class KafkaSink {

  val logger: Logger = Logger.getLogger(classOf[KafkaSink].getName)
  val intermediateTopic: String =
    s"intermediate-topic-${UUID.randomUUID().toString}"

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
                  sparkConfig: SparkConfigs,
                  kafkaConfigs: KafkaConfigs): Either[Throwable, Any] = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](props)
    KafkaUtils.verifyTopicExistence(topic,
                                    kafkaConfigs.zookeeperUrls,
                                    bootstrapServers,
                                    enableTopicCreation = true)
    kafkaMode match {
      case SimpleProducer =>
        send(jsonPath, topic, producer)
      case KafkaStreaming(streamAppId) =>
        KafkaUtils.verifyTopicExistence(intermediateTopic,
                                        kafkaConfigs.zookeeperUrls,
                                        bootstrapServers,
                                        enableTopicCreation = true)
        send(jsonPath, intermediateTopic, producer)
        runStream(streamAppId, intermediateTopic, bootstrapServers, topic)
      case SparkKafkaProducerPlugin(useStream,
                                    intermediateTopic,
                                    checkpointFolder) =>
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
        throw new RuntimeException("This mode is not supported while producing data. The supported Kafka Consume Mode are " +
          ": 'SimpleProducer', 'KafkaStreaming' and 'SparkKafkaProducerPlugin'.")
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
    //todo specify explicitly your imports
    import org.apache.spark.sql.functions._
    DataFrameUtils(sparkConfig)
      .loadDataFrame(jsonPath, JSON)
      .map(df => {
        val intermediateData =
          df.withColumn("technical_uuid", lit(UUID.randomUUID().toString))
        intermediateData
          .select(col("technical_uuid").cast("string").as("key"),
                  to_json(struct("*")).as("value"))
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

      send(jsonPath, intermediateTopic, producer)

      val kafkaStream = DataFrameUtils(sparkConfig).sparkSession.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("startingOffsets", offset.value)
        .option("subscribe", intermediateTopic)
        .load()

      val intermediateDf =
        kafkaStream.withColumn("technical_uuid",
                               lit(UUID.randomUUID().toString))
      logger.info(
        s"Sending data through Spark Kafka Streaming Plugin to '$intermediateTopic'.")
      intermediateDf
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
        s"Sending data through Kafka streams to '$intermediateTopic' topic - Sent data: '$streamsOutputTopic'")

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
    Try {
      for (line <- getJsonLines(jsonPath)) {
        val uuid = UUID.randomUUID().toString
        val data =
          new ProducerRecord[String, String](topic, uuid, line)
        producer.send(data)
        logger.info(s"Topic: '$topic' - Sent data: '$line'")
      }
      producer.close()
    }.toEither
      .leftMap(thr =>
        KafkaError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Get lines from json file
    * @param jsonPath The path that contains json data to be send
    * @return an Iterator of String
    */
  private def getJsonLines(jsonPath: String): Iterator[String] = {
    val jsonFile = Source.fromFile(jsonPath)
    jsonFile.getLines
  }
}
