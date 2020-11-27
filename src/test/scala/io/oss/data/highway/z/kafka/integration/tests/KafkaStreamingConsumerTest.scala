package io.oss.data.highway.z.kafka.integration.tests

import io.oss.data.highway.configuration.SparkConfigs
import io.oss.data.highway.converter.KafkaSampler
import io.oss.data.highway.model.{Earliest, INFO, JSON, KafkaStreaming}

object KafkaStreamingConsumerTest {

  def main(args: Array[String]): Unit = {
    // todo It consumes only 50% of messages .. to be investigated !
    val in = "kafka-to-json-topic-2"
    val out =
      "/home/ghazi/workspace/data-highway/src/test/resources/output/files"
    val sparkConfig = SparkConfigs("app-name", "local[*]", INFO)

    KafkaSampler.peek(in,
                      out,
                      Some(JSON),
                      KafkaStreaming("stream-app-id"),
                      "localhost:9092",
                      Earliest,
                      "consumer-group",
                      sparkConfig)
  }
}
