package io.oss.data.highway.z.kafka.integration.tests

import io.oss.data.highway.configuration.SparkConfigs
import io.oss.data.highway.converter.KafkaSink
import io.oss.data.highway.model.{
  Latest,
  PureKafkaProducer,
  PureKafkaStreamsProducer,
  WARN
}
import org.apache.log4j.BasicConfigurator

object SimpleProducerTest {

  val sparkConfig: SparkConfigs =
    SparkConfigs("handler-app-test", "local[*]", WARN)

  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    val in = "src/test/resources/file_to_kafka-data/input/data.json"
    val out = "kafka-to-json-topic-2"
    val brokerUrl = "localhost:9092"

    new KafkaSink().publishToTopic(
      in,
      out,
      brokerUrl,
      PureKafkaStreamsProducer("stream-app-id", Latest),
      sparkConfig)
  }
}
