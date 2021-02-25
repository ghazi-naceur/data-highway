package io.oss.data.highway.z.kafka.integration.tests

import io.oss.data.highway.sinks.KafkaSampler
import io.oss.data.highway.models.{Earliest, JSON, PureKafkaConsumer}

object SimpleConsumerTest {

  def main(args: Array[String]): Unit = {

    val in = "kafka-to-json-topic-2"
    val out =
      "/home/ghazi/workspace/data-highway/src/test/resources/output/files"

    KafkaSampler.consumeFromTopic(in,
                                  out,
                                  PureKafkaConsumer("localhost:9092",
                                                    "consumer-group",
                                                    Earliest,
                                                    Some(JSON)))
  }
}
