package io.oss.data.highway.z.kafka.integration.tests

import io.oss.data.highway.sinks.KafkaSampler
import io.oss.data.highway.models.{Earliest, JSON, SparkKafkaPluginConsumer}

object SparkKafkaConsumerPluginTest {

  def main(args: Array[String]): Unit = {

    val in = "kafka-to-json-topic-3"
    val out =
      "/home/ghazi/workspace/data-highway/src/test/resources/output/files"

    KafkaSampler.consumeFromTopic(
      in,
      out,
      SparkKafkaPluginConsumer("localhost:9092", Earliest, Some(JSON)))
  }
}
