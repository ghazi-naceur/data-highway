package io.oss.data.highway.zoo.manual.tests

import io.oss.data.highway.converter.KafkaSink
import io.oss.data.highway.model.{KafkaStreaming, Latest, SparkKafkaPlugin}
import org.apache.log4j.BasicConfigurator

object SparkKafkaPluginTest {

  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    val in = "src/test/resources/json_to_kafka-data/input/data.json"
    val out = "json-to-kafka-topic-2"
    val brokerUrl = "localhost:9092"

    new KafkaSink().sendToTopic(
      in,
      out,
      brokerUrl,
      SparkKafkaPlugin(useStream = false,
                       "intermediate-skp",
                       "/tmp/data-highway/checkpoint-4"))
  }
  // consumer : ./kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic json-to-kafka-topic-2
}
