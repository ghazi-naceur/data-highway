package io.oss.data.highway.z.kafka.integration.tests

import io.oss.data.highway.configuration.SparkConfigs
import io.oss.data.highway.converter.KafkaSampler
import io.oss.data.highway.model.{Earliest, INFO, JSON, PureKafkaConsumer}

object SimpleConsumerTest {

  def main(args: Array[String]): Unit = {

    val in = "kafka-to-json-topic-2"
    val out =
      "/home/ghazi/workspace/data-highway/src/test/resources/output/files"
    val sparkConfig = SparkConfigs("app-name", "local[*]", INFO)

    KafkaSampler.consumeFromTopic(in,
                                  out,
                                  Some(JSON),
                                  PureKafkaConsumer("consumer-group", Earliest),
                                  "localhost:9092",
                                  sparkConfig)
  }
}
