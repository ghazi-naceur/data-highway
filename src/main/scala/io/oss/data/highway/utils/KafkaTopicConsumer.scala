package io.oss.data.highway.utils

import java.time.Duration
import java.util
import java.util.Properties

import io.oss.data.highway.model.Offset
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import scala.jdk.CollectionConverters._
import org.apache.log4j.Logger
import cats.syntax.either._
import io.oss.data.highway.model.DataHighwayError.KafkaError

object KafkaTopicConsumer {

  val logger: Logger = Logger.getLogger(classOf[App].getName)

  def consumeFromKafka(topic: String,
                       brokerUrls: String,
                       offset: Offset,
                       consumerGroup: String): Either[KafkaError, Unit] = {
    for {
      consumer <- consume(topic, brokerUrls, offset, consumerGroup)
      _ = while (true) {
        val record = consumer.poll(Duration.ofSeconds(5)).asScala
        logger.info("=======> Consumer :")
        for (data <- record.iterator)
          logger.info(
            "Topic: " + data.topic() +
              ",Key: " + data.key() +
              ",Value: " + data.value() +
              ", Offset: " + data.offset() +
              ", Partition: " + data.partition())
      }
    } yield ()

  }

  def consume(topic: String,
              brokerUrls: String,
              offset: Offset,
              consumerGroup: String)
    : Either[KafkaError, KafkaConsumer[String, String]] = {
    val props = new Properties()
    props.put("bootstrap.servers", brokerUrls)
    props.put("key.deserializer", classOf[StringDeserializer].getName)
    props.put("value.deserializer", classOf[StringDeserializer].getName)
    props.put("auto.offset.reset", offset.value)
    props.put("group.id", consumerGroup)
    Either
      .catchNonFatal {
        val consumer: KafkaConsumer[String, String] =
          new KafkaConsumer[String, String](props)
        consumer.subscribe(util.Arrays.asList(topic))
        consumer
      }
      .leftMap(thr =>
        KafkaError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }
}
