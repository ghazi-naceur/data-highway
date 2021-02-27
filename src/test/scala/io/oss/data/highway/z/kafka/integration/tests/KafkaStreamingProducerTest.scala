package io.oss.data.highway.z.kafka.integration.tests

import io.oss.data.highway.sinks.KafkaSink
import io.oss.data.highway.models.{Latest, PureKafkaStreamsProducer}
import org.apache.log4j.BasicConfigurator

object KafkaStreamingProducerTest {

  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    val in = "src/test/resources/file_to_kafka-data/input/data.json"
    val out = "kafka-to-json-topic-2"

    KafkaSink.publishToTopic(
      in,
      out,
      PureKafkaStreamsProducer("localhost:9092", "stream-app-id", Latest))
  }
}
