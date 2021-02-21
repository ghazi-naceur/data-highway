package io.oss.data.highway.z.kafka.integration.tests

import io.oss.data.highway.converter.KafkaSampler
import io.oss.data.highway.model.{Earliest, JSON, PureKafkaConsumer}

object KafkaStreamingConsumerTest {

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
