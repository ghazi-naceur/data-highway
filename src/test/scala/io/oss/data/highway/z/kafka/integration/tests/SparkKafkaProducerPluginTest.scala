package io.oss.data.highway.z.kafka.integration.tests

import io.oss.data.highway.configuration.SparkConfigs
import io.oss.data.highway.converter.KafkaSink
import io.oss.data.highway.model.{SparkKafkaPluginProducer, WARN}
import org.apache.log4j.BasicConfigurator

object SparkKafkaProducerPluginTest {

  val sparkConfig: SparkConfigs =
    SparkConfigs("handler-app-test", "local[*]", WARN)

  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    val in = "src/test/resources/file_to_kafka-data/input/data.json"
    val out = "kafka-to-json-topic-3"

    new KafkaSink().publishToTopic(in,
                                   out,
                                   SparkKafkaPluginProducer("localhost:9092"),
                                   sparkConfig)
  }
}
