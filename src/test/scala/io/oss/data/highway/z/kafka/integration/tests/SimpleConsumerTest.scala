package io.oss.data.highway.z.kafka.integration.tests

import io.oss.data.highway.configuration.SparkConfig
import io.oss.data.highway.converter.KafkaSampler
import io.oss.data.highway.model.{Earliest, INFO, JSON, SimpleConsumer}
object SimpleConsumerTest {

  def main(args: Array[String]): Unit = {

    val in = "kafka-to-json-topic-out"
    val out =
      "/home/ghazi/workspace/data-highway/src/test/resources/output/files"
    val sparkConfig = SparkConfig("app-name", "local[*]", INFO)

    KafkaSampler.peek(in,
                      out,
                      Some(JSON),
                      SimpleConsumer,
                      "localhost:9092",
                      Earliest,
                      "consumer-group",
                      sparkConfig)
  }
}
