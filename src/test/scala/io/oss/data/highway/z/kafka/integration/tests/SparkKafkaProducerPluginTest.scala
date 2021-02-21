package io.oss.data.highway.z.kafka.integration.tests

import io.oss.data.highway.converter.KafkaSink
import io.oss.data.highway.model.{SparkKafkaPluginProducer}
import org.apache.log4j.BasicConfigurator

object SparkKafkaProducerPluginTest {

  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    val in = "src/test/resources/file_to_kafka-data/input/data.json"
    val out = "kafka-to-json-topic-3"

    new KafkaSink().publishToTopic(in,
                                   out,
                                   SparkKafkaPluginProducer("localhost:9092"))
  }
}
