package io.oss.data.highway.z.kafka.integration.tests

import io.oss.data.highway.configuration.SparkConfigs
import io.oss.data.highway.converter.KafkaSampler
import io.oss.data.highway.model.{
  Earliest,
  INFO,
  JSON,
  SparkKafkaPluginConsumer
}

object SparkKafkaConsumerPluginTest {

  def main(args: Array[String]): Unit = {

    val in = "kafka-to-json-topic-3"
    val out =
      "/home/ghazi/workspace/data-highway/src/test/resources/output/files"
    val sparkConfig = SparkConfigs("app-name", "local[*]", INFO)

    KafkaSampler.consumeFromTopic(
      in,
      out,
      Some(JSON),
      SparkKafkaPluginConsumer("localhost:9092", Earliest),
      sparkConfig)
  }
}
