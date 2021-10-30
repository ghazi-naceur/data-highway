package gn.oss.data.highway.engine

import gn.oss.data.highway.helper.TestHelper
import gn.oss.data.highway.models.DataHighwayErrorObj.KafkaError
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.spark.sql.SaveMode
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.io.File
import java.nio.file.Files

class KafkaExtractorSpec
    extends AnyWordSpecLike
    with BeforeAndAfterEach
    with Matchers
    with EmbeddedKafka
    with TestHelper {

  override def afterEach(): Unit = {
    deleteFolderWithItsContent("/tmp/data-highway")
  }

  "KafkaSampler.sinkWithPureKafka" should {
    "sink topic content using Pure Kafka Consumer" in {
      EmbeddedKafka.start()
      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
      val time              = System.currentTimeMillis().toString
      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
//        val port = actualConfig.kafkaPort
//        publishStringMessageToKafka(s"topic-$time", "{\"some-key\":\"some value\"}")
//        publishStringMessageToKafka(s"topic-$time", "{\"some-key\":\"some value\"}")
//        publishStringMessageToKafka(s"topic-$time", "{\"some-key\":\"some value\"}")
//        val path = "/tmp/data-highway/test/"
//        Files.createDirectories(new java.io.File(path).toPath)
//        KafkaSampler.sinkWithPureKafka(
//          s"topic-$time",
//          s"/tmp/data-highway/kafka-to-file/$time",
//          "",
//          io.oss.data.highway.models.File(JSON, path),
//          Local,
//          SaveMode.Overwrite,
//          s"localhost:$port",
//          Earliest,
//          "consumer-group-1",
//          null
//        )
//
//        val result = FilesUtils.listFiles(List(s"/tmp/data-highway/kafka-to-file/$time"))
//        result.right.get.size shouldBe 3
      }
    }
    EmbeddedKafka.stop()
  }

  "KafkaSampler.sinkWithPureKafka" should {
    "throw an exception" in {
      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
//        val result = KafkaSampler.sinkWithPureKafka("", "", Local, "", Earliest, "", null)
//        result.left.get shouldBe a[KafkaError]
      }
    }
    EmbeddedKafka.stop()
  }

  "KafkaSampler.sinkViaSparkKafkaPlugin" should {
    "sink topic content using Spark Kafka Plugin Consumer" in {
      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
      val time              = System.currentTimeMillis().toString
      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
//        val port = actualConfig.kafkaPort
//        publishStringMessageToKafka(s"topic-$time", "{\"some-key\":\"some value\"}")
//        publishStringMessageToKafka(s"topic-$time", "{\"some-key\":\"some value\"}")
//        publishStringMessageToKafka(s"topic-$time", "{\"some-key\":\"some value\"}")
//        KafkaSampler.sinkViaSparkKafkaPlugin(
//          DataFrameUtils.sparkSession,
//          s"topic-$time",
//          s"/tmp/data-highway/kafka-to-file/$time",
//          Local,
//          s"localhost:$port",
//          Earliest
//        )
//
//        val result = FilesUtils.listFiles(List(s"/tmp/data-highway/kafka-to-file/$time"))
//        result.right.get.size shouldBe 3
      }
    }
    EmbeddedKafka.stop()
  }
}
