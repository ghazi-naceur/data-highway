package io.oss.data.highway.z.kafka.integration.tests

import io.oss.data.highway.configuration.{KafkaConfigs, SparkConfigs}
import io.oss.data.highway.converter.KafkaSink
import io.oss.data.highway.model.{SparkKafkaProducerPlugin, WARN}
import org.apache.log4j.BasicConfigurator

object SparkKafkaProducerPluginTest {

  val sparkConfig: SparkConfigs =
    SparkConfigs("handler-app-test", "local[*]", WARN)

  val kafkaConfig: KafkaConfigs = KafkaConfigs("localhost:2181")

  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    val in = "src/test/resources/json_to_kafka-data/input/data.json"
    val out = "kafka-to-json-topic-out"
    val brokerUrl = "localhost:9092"

    new KafkaSink().sendToTopic(
      in,
      out,
      brokerUrl,
      SparkKafkaProducerPlugin(useStream = true,
                               "intermediate-skp-23",
                               "/tmp/data-highway/checkpoint-23"),
      sparkConfig,
      kafkaConfig)
  }
  // consumer : ./kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic kafka-to-json-topic-out
}
