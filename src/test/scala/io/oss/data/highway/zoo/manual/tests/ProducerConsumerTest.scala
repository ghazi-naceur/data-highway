package io.oss.data.highway.zoo.manual.tests

import io.oss.data.highway.converter.KafkaSink
import io.oss.data.highway.model.{Latest, ProducerConsumer}
import org.apache.log4j.BasicConfigurator

object ProducerConsumerTest {

  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    val in = "src/test/resources/json_to_kafka-data/input/data.json"
    val out = "json-to-kafka-topic"
    val brokerUrl = "localhost:9092"

    new KafkaSink().sendToTopic(
      in,
      out,
      brokerUrl,
      ProducerConsumer(useConsumer = true, Latest, "consumer-group-pc"))
  }
}
