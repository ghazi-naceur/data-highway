package io.oss.data.highway.z.kafka.integration.tests

import io.oss.data.highway.configuration.SparkConfig
import io.oss.data.highway.converter.KafkaSink
import io.oss.data.highway.model.{Latest, SimpleProducer, WARN}
import org.apache.log4j.BasicConfigurator

object SimpleProducerTest {

  val sparkConfig: SparkConfig =
    SparkConfig("handler-app-test", "local[*]", WARN)

  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    val in = "src/test/resources/json_to_kafka-data/input/data.json"
    val out = "topic-1"
    val brokerUrl = "localhost:9092"

    new KafkaSink().sendToTopic(in, out, brokerUrl, SimpleProducer, sparkConfig)
  }
}