package io.oss.data.highway.converter

import java.time.Duration

import org.apache.kafka.clients.producer._
import java.util.{Properties, UUID}

import io.oss.data.highway.model.{
  JSON,
  KafkaMode,
  KafkaStreaming,
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
import io.oss.data.highway.utils.DataFrameUtils.sparkSession
import org.apache.spark.sql.functions.lit

import scala.sys.ShutdownHookThread

class KafkaSink {

  val logger: Logger = Logger.getLogger(classOf[KafkaSink].getName)

  def sendToTopic(jsonPath: String,
                  topic: String,
                  bootstrapServers: String,
                  kafkaMode: KafkaMode): Either[Throwable, Any] = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](props)
    kafkaMode match {
      case ProducerConsumer(useConsumer, offset, consumerGroup) =>
        send(jsonPath, topic, producer)
        consume(topic, bootstrapServers, useConsumer, offset, consumerGroup)
      case KafkaStreaming(streamsOutputTopic,
                          useConsumer,
                          offset,
                          consumerGroup) =>
        send(jsonPath, topic, producer)
        runStream(topic,
                  bootstrapServers,
                  streamsOutputTopic,
                  useConsumer,
                  offset,
                  consumerGroup)
      case SparkKafkaPlugin(useConsumer, offset, consumerGroup, useStream) =>
        // TODO consumer is ont implemented yet for both cases
        if (useStream) {
          sendUsingStreamSparkKafkaPlugin(jsonPath,
                                          producer,
                                          bootstrapServers,
                                          topic)
        } else {
          sendUsingSparkKafkaPlugin(jsonPath, bootstrapServers, topic)
        }
    }
  }

  private def sendUsingSparkKafkaPlugin(
      jsonPath: String,
      bootstrapServers: String,
      topic: String): Either[Throwable, Unit] = {
    import org.apache.spark.sql.functions._
    DataFrameUtils
      .loadDataFrame(jsonPath, JSON)
      .map(df => {
        val dff = df.withColumn("uuid", lit(UUID.randomUUID().toString))
        dff
          .select(col("uuid").cast("string").as("key"),
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
      topic: String): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      // Use an intermediate topic
      send(jsonPath, "json-to-kafka-streaming-topic", producer)

      val kafkaStream = sparkSession.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("startingOffsets", "latest")
        .option("subscribe", "json-to-kafka-streaming-topic")
        .load()

      val dff =
        kafkaStream.withColumn("uuid", lit(UUID.randomUUID().toString))
      dff
        .selectExpr("CAST(value AS STRING)")
        .writeStream
        .format("kafka") // console
        .option("truncate", false)
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("checkpointLocation", "/tmp/data-highway/checkpoint")
        .option("topic", topic)
        .start()
        .awaitTermination()
    }
  }

  private def runStream(
      topic: String,
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
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-app")
      props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass)
      props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass)
      val builder = new StreamsBuilder

      val dataKStream = builder.stream[String, String](topic)
      dataKStream.to(streamsOutputTopic)

      val streams = new KafkaStreams(builder.build(), props)

      streams.start()

      consume(streamsOutputTopic,
              bootstrapServers,
              useConsumer,
              offset,
              consumerGroup)

      sys.ShutdownHookThread {
        streams.close(Duration.ofSeconds(10))
      }
    }
  }

  private def consume(topic: String,
                      bootstrapServers: String,
                      useConsumer: Boolean,
                      offset: Offset,
                      consumerGroup: String): Either[Throwable, Any] = {
    Either.catchNonFatal {
      if (useConsumer) {
        KafkaTopicConsumer.consumeFromKafka(topic,
                                            bootstrapServers,
                                            offset,
                                            consumerGroup)
      }
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
