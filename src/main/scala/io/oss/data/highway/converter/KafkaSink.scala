package io.oss.data.highway.converter

import java.time.Duration

import org.apache.kafka.clients.producer._
import java.util.{Properties, UUID}

import io.oss.data.highway.model.{
  KafkaMode,
  KafkaStreaming,
  Offset,
  ProducerConsumer,
  SparkKafkaPlugin
}
import io.oss.data.highway.utils.KafkaTopicConsumer
import org.apache.kafka.common.serialization.{Serdes, StringSerializer}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import scala.io.Source
import scala.util.Try
import org.apache.log4j.Logger
import cats.syntax.either._

class KafkaSink {

  val logger: Logger = Logger.getLogger(classOf[KafkaSink].getName)

  def sendToTopic(jsonPath: String,
                  topic: String,
                  bootstrapServers: String,
                  useConsumer: Boolean,
                  offset: Offset,
                  consumerGroup: String,
                  kafkaMode: KafkaMode): Either[Throwable, Any] = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](props)
    kafkaMode match {
      case ProducerConsumer =>
        send(jsonPath,
             topic,
             bootstrapServers,
             useConsumer,
             offset,
             consumerGroup,
             producer,
             kafkaMode)
      case KafkaStreaming(streamsOutputTopic) =>
        send(jsonPath,
             topic,
             bootstrapServers,
             useConsumer,
             offset,
             consumerGroup,
             producer,
             kafkaMode)
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

          sys.ShutdownHookThread {
            streams.close(Duration.ofSeconds(10))
          }
        }
      case SparkKafkaPlugin =>
        ???
    }

  }

  private def send(jsonPath: String,
                   topic: String,
                   bootstrapServers: String,
                   useConsumer: Boolean,
                   offset: Offset,
                   consumerGroup: String,
                   producer: KafkaProducer[String, String],
                   kafkaMode: KafkaMode): Either[Throwable, Any] = {
    Try {
      for (line <- getJsonLines(jsonPath)) {
        val uuid = UUID.randomUUID().toString
        val data =
          new ProducerRecord[String, String](topic, uuid, line)
        producer.send(data)
        logger.info(line)
      }
      producer.close()

      if (useConsumer && kafkaMode == ProducerConsumer) {
        KafkaTopicConsumer.consumeFromKafka(topic,
                                            bootstrapServers,
                                            offset,
                                            consumerGroup)
      }
    }.toEither
  }

  private def getJsonLines(jsonPath: String): Iterator[String] = {
    val jsonFile = Source.fromFile(jsonPath)
    jsonFile.getLines
  }
}
