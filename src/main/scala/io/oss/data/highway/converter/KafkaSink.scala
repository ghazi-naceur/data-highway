package io.oss.data.highway.converter

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.kafka.clients.producer._
import java.util.Properties

import io.oss.data.highway.utils.Constants.DateTimePattern
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source
import scala.util.Try
import org.apache.log4j.Logger

class KafkaSink {

  val logger: Logger = Logger.getLogger(classOf[KafkaSink].getName)

  def sendToTopic(jsonPath: String,
                  topic: String,
                  bootstrapServers: String): Either[Throwable, Unit] = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](props)
    val timestamp =
      LocalDateTime.now.format(DateTimeFormatter.ofPattern(DateTimePattern))

    Try {
      for (line <- getJsonLines(jsonPath)) {
        logger.info(line)
        val data =
          new ProducerRecord[String, String](topic, timestamp, line)
        producer.send(data)
      }
      producer.close()
    }.toEither
  }

  private def getJsonLines(jsonPath: String): Iterator[String] = {
    val jsonFile = Source.fromFile(jsonPath)
    jsonFile.getLines
  }
}
