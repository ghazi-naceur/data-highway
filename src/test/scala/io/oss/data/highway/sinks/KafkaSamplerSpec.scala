package io.oss.data.highway.sinks

import io.oss.data.highway.models.DataHighwayError.KafkaError
import io.oss.data.highway.models.{Earliest, Local}
import io.oss.data.highway.utils.{DataFrameUtils, FilesUtils}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class KafkaSamplerSpec extends AnyWordSpecLike with Matchers with EmbeddedKafka {

  "KafkaSampler.sinkWithPureKafka" should {
    "sink topic content using Pure Kafka Consumer" in {
      EmbeddedKafka.start()
      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
      val time              = System.currentTimeMillis().toString
      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
        val port = actualConfig.kafkaPort
        publishStringMessageToKafka(s"topic-$time", "{\"some-key\":\"some value\"}")
        publishStringMessageToKafka(s"topic-$time", "{\"some-key\":\"some value\"}")
        publishStringMessageToKafka(s"topic-$time", "{\"some-key\":\"some value\"}")
        KafkaSampler.sinkWithPureKafka(
          s"topic-$time",
          s"/tmp/data-highway/kafka-to-file/$time",
          Local,
          s"localhost:$port",
          Earliest,
          "consumer-group-1",
          null
        )

        val result = FilesUtils.listFiles(List(s"/tmp/data-highway/kafka-to-file/$time"))
        result.right.get.size shouldBe 3
      }
    }
    EmbeddedKafka.stop()
  }

  "KafkaSampler.sinkWithPureKafka" should {
    "throw an exception" in {
      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
        val result = KafkaSampler.sinkWithPureKafka("", "", Local, "", Earliest, "", null)
        result.left.get shouldBe a[KafkaError]
      }
    }
    EmbeddedKafka.stop()
  }

  "KafkaSampler.sinkViaSparkKafkaPlugin" should {
    "sink topic content using Spark Kafka Plugin Consumer" in {
      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
      val time              = System.currentTimeMillis().toString
      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
        val port = actualConfig.kafkaPort
        publishStringMessageToKafka(s"topic-$time", "{\"some-key\":\"some value\"}")
        publishStringMessageToKafka(s"topic-$time", "{\"some-key\":\"some value\"}")
        publishStringMessageToKafka(s"topic-$time", "{\"some-key\":\"some value\"}")
        KafkaSampler.sinkViaSparkKafkaPlugin(
          DataFrameUtils.sparkSession,
          s"topic-$time",
          s"/tmp/data-highway/kafka-to-file/$time",
          Local,
          s"localhost:$port",
          Earliest
        )

        val result = FilesUtils.listFiles(List(s"/tmp/data-highway/kafka-to-file/$time"))
        result.right.get.size shouldBe 3
      }
    }
    EmbeddedKafka.stop()
  }
}
