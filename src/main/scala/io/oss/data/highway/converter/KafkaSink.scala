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
  ProducerConsumer,
  SparkKafkaPlugin
}
import io.oss.data.highway.utils.{DataFrameUtils, KafkaTopicConsumer}
import org.apache.kafka.common.serialization.{Serdes, StringSerializer}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import scala.io.Source
import scala.util.Try
import org.apache.log4j.Logger
import cats.syntax.either._
import io.oss.data.highway.configuration.SparkConfig
import org.apache.spark.sql.functions.lit

import scala.sys.ShutdownHookThread

class KafkaSink {

  val logger: Logger = Logger.getLogger(classOf[KafkaSink].getName)
  val intermediateTopic: String =
    s"intermediate-topic-${UUID.randomUUID().toString}"
  def sendToTopic(jsonPath: String,
                  topic: String,
                  bootstrapServers: String,
                  kafkaMode: KafkaMode,
                  sparkConfig: SparkConfig): Either[Throwable, Any] = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](props)
    kafkaMode match {
      case ProducerConsumer(useConsumer, offset, consumerGroup) =>
        send(jsonPath, topic, producer)
        Try(if (useConsumer) {
          consume(topic, bootstrapServers, offset, consumerGroup)
        }).toEither
      case KafkaStreaming(streamAppId, useConsumer, offset, consumerGroup) =>
        send(jsonPath, intermediateTopic, producer)
        runStream(streamAppId,
                  intermediateTopic,
                  bootstrapServers,
                  topic,
                  useConsumer,
                  offset,
                  consumerGroup)
      case SparkKafkaPlugin(useStream, intermediateTopic, checkpointFolder) =>
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
    }
  }

  private def sendUsingSparkKafkaPlugin(
      jsonPath: String,
      bootstrapServers: String,
      topic: String,
      sparkConfig: SparkConfig): Either[Throwable, Unit] = {
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
      })
  }

  private def sendUsingStreamSparkKafkaPlugin(
      jsonPath: String,
      producer: KafkaProducer[String, String],
      bootstrapServers: String,
      outputTopic: String,
      intermediateTopic: String,
      checkpointFolder: String,
      sparkConfig: SparkConfig,
      offset: Offset = Latest): Either[Throwable, Unit] = {
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

  private def runStream(
      streamAppId: String,
      intermediateTopic: String,
      bootstrapServers: String,
      streamsOutputTopic: String,
      useConsumer: Boolean,
      offset: Offset,
      consumerGroup: String): Either[Throwable, ShutdownHookThread] = {
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

      streams.start()

      if (useConsumer) {
        consume(streamsOutputTopic, bootstrapServers, offset, consumerGroup)
      }

      sys.ShutdownHookThread {
        streams.close(Duration.ofSeconds(10))
      }
    }
  }

  private def consume(topic: String,
                      bootstrapServers: String,
                      offset: Offset,
                      consumerGroup: String): Either[Throwable, Any] = {
    Either.catchNonFatal {
      KafkaTopicConsumer.consumeFromKafka(topic,
                                          bootstrapServers,
                                          offset,
                                          consumerGroup)
    }
  }

  private def send(
      jsonPath: String,
      topic: String,
      producer: KafkaProducer[String, String]): Either[Throwable, Any] = {
    Try {
      for (line <- getJsonLines(jsonPath)) {
        val uuid = UUID.randomUUID().toString
        val data =
          new ProducerRecord[String, String](topic, uuid, line)
        producer.send(data)
        logger.info(line)
      }
      producer.close()
    }.toEither
  }

  private def getJsonLines(jsonPath: String): Iterator[String] = {
    val jsonFile = Source.fromFile(jsonPath)
    jsonFile.getLines
  }
}
