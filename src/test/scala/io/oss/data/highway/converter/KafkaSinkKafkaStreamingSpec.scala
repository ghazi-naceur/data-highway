package io.oss.data.highway.converter

import io.oss.data.highway.configuration.SparkConfigs
import io.oss.data.highway.model.{
  INFO,
  PureKafkaProducer,
  PureKafkaStreamsProducer
}
import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.wordspec.AnyWordSpec

class KafkaSinkKafkaStreamingSpec extends AnyWordSpec with EmbeddedKafka {
//  In-memory Zookeeper and Kafka will be instantiated respectively on port 6000 and 6001 and automatically shutdown at the end of the test.
  val kafkaSink = new KafkaSink()
  val in = "src/test/resources/file_to_kafka-data/input/data.json"
  val out2 = "kafka-to-json-topic-2"
  val brokerUrl = "localhost:6001"
  val storagePath =
    "src/test/resources/output/files"
  val sparkConfig: SparkConfigs = SparkConfigs("app-name", "local[*]", INFO)

  "runs with embedded kafka" should {
    "work using Pure Kafka Producer with streaming" in {
      withRunningKafka {
        kafkaSink.publishToTopic(in,
                                 out2,
                                 brokerUrl,
                                 PureKafkaStreamsProducer("stream-app-id"),
                                 sparkConfig)
        assert(consumeFirstStringMessageFrom(out2).nonEmpty)
      }
    }
  }
}
