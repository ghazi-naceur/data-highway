package io.oss.data.highway.converter

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.kafka.clients.producer._
import java.util.Properties

import io.oss.data.highway.model.{
  KafkaMode,
  KafkaStreams,
  Offset,
  ProducerConsumer,
  SparkKafkaPlugin
}
import io.oss.data.highway.utils.Constants.DateTimePattern
import io.oss.data.highway.utils.KafkaTopicConsumer
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source
import scala.util.Try
import org.apache.log4j.Logger

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
    val timestamp =
      LocalDateTime.now.format(DateTimeFormatter.ofPattern(DateTimePattern))

    kafkaMode match {
      case ProducerConsumer =>
        Try {
          for (line <- getJsonLines(jsonPath)) {
            logger.info(line)
            val data =
              new ProducerRecord[String, String](topic, timestamp, line)
            producer.send(data)
          }
          producer.close()

          if (useConsumer) {
            KafkaTopicConsumer.consumeFromKafka(topic,
                                                bootstrapServers,
                                                offset,
                                                consumerGroup)
          }
        }.toEither
      case KafkaStreams =>
        ???
      case SparkKafkaPlugin =>
        ???
    }

  }

  private def getJsonLines(jsonPath: String): Iterator[String] = {
    val jsonFile = Source.fromFile(jsonPath)
    jsonFile.getLines
  }
}
