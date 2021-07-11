package io.oss.data.highway.z.kafka.integration.tests

import io.oss.data.highway.sinks.KafkaSink
import io.oss.data.highway.models.{Local, SparkKafkaPluginProducer}
import org.apache.log4j.BasicConfigurator

object SparkKafkaProducerPluginTest {

  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    val in  = "src/test/resources/file_to_kafka-data/input/data.json"
    val out = "kafka-to-json-topic-3"

    KafkaSink
      .publishToTopic(in, out, Local, SparkKafkaPluginProducer("localhost:9092"))
  }
}
