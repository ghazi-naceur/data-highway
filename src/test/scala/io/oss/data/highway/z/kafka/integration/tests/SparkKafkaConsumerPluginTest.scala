package io.oss.data.highway.z.kafka.integration.tests

import io.oss.data.highway.configuration.{KafkaConfigs, SparkConfigs}
import io.oss.data.highway.converter.KafkaSampler
import io.oss.data.highway.model.{
  Earliest,
  INFO,
  JSON,
  SparkKafkaConsumerPlugin
}

object SparkKafkaConsumerPluginTest {

  def main(args: Array[String]): Unit = {

    // todo It doesn't consume all data when "useStream = false"
    val in = "kafka-to-json-topic-out"
    val out =
      "/home/ghazi/workspace/data-highway/src/test/resources/output/files"
    val sparkConfig = SparkConfigs("app-name", "local[*]", INFO)
    val kafkaConfig: KafkaConfigs = KafkaConfigs("localhost:2181")

    KafkaSampler.peek(in,
                      out,
                      Some(JSON),
                      SparkKafkaConsumerPlugin(useStream = false),
                      "localhost:9092",
                      Earliest,
                      "consumer-group",
                      sparkConfig,
                      kafkaConfig)
  }
}
