package gn.oss.data.highway.engine

import gn.oss.data.highway.engine.sinks.KafkaSink
import gn.oss.data.highway.helper.TestHelper
import gn.oss.data.highway.models.Earliest
import gn.oss.data.highway.utils.FilesUtils
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.matchers.should.Matchers
import org.scalatest._
import wordspec._

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.Files

class KafkaSinkSpec
    extends AnyWordSpecLike
    with Matchers
    with BeforeAndAfterEach
    with EmbeddedKafka
    with TestHelper {

  override def afterEach(): Unit = {
    deleteFolderWithItsContent("/tmp/data-highway")
  }

  "KafkaSink.publishFileContent" should {
    "publish file content using Pure Kafka Producer" in {
      EmbeddedKafka.start()
      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
      val time              = System.currentTimeMillis().toString
      val srcPath           = s"/tmp/data-highway/input-$time/dataset-$time"
      Files.createDirectories(new File(srcPath).toPath)
      val fstream = new FileWriter(srcPath + "/file.json", true)
      val out     = new BufferedWriter(fstream)
      out.write(
        "{\"some-key\":\"some value\"}\n{\"some-key\":\"some value\"}\n{\"some-key\":\"some value\"}"
      )
      out.close()
      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
        import scala.collection.JavaConverters.mapAsJavaMapConverter
        val port = actualConfig.kafkaPort
        val map: Map[String, AnyRef] = Map[String, String](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:$port",
          ProducerConfig.MAX_BLOCK_MS_CONFIG      -> 10000.toString,
          ProducerConfig.RETRY_BACKOFF_MS_CONFIG  -> 1000.toString
        ) ++ actualConfig.customProducerProperties

        val producer = new KafkaProducer[String, String](
          map.asJava,
          new StringSerializer(),
          new StringSerializer()
        )
        KafkaSink.publishFileContent(
          List(
            "{\"some-key\":\"some value\"}",
            "{\"some-key\":\"some value\"}",
            "{\"some-key\":\"some value\"}"
          ),
          "topic-1",
          producer
        )

        val result = FilesUtils.listFiles(List(s"/tmp/data-highway/processed/dataset-$time"))
        consumeFirstStringMessageFrom("topic-1") shouldBe "{\"some-key\":\"some value\"}"
      }
    }
    EmbeddedKafka.stop()
  }

  "KafkaSink.publishFileContent" should {
    "throw an exception" in {
      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
        import scala.collection.JavaConverters.mapAsJavaMapConverter
        val port = actualConfig.kafkaPort
        val map: Map[String, AnyRef] = Map[String, String](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> s"localhost:$port",
          ProducerConfig.MAX_BLOCK_MS_CONFIG      -> 10000.toString,
          ProducerConfig.RETRY_BACKOFF_MS_CONFIG  -> 1000.toString
        ) ++ actualConfig.customProducerProperties

        val producer = new KafkaProducer[String, String](
          map.asJava,
          new StringSerializer(),
          new StringSerializer()
        )
        val result = KafkaSink.publishFileContent(
          List(""),
          null,
          producer
        )

        result.left.get shouldBe a[Throwable]
      }
    }
    EmbeddedKafka.stop()
  }

  "KafkaSink.runStream" should {
    "run stream using Kafka Streams and send content from one topic to another" in {
      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
        val port = actualConfig.kafkaPort
        publishStringMessageToKafka("topic-3", "{\"some-key\":\"some value\"}")
        KafkaSink.runStream("stream-app-2", "topic-3", s"localhost:$port", "topic-4", Earliest)
        consumeFirstStringMessageFrom("topic-4") shouldBe "{\"some-key\":\"some value\"}"
      }
    }
    EmbeddedKafka.stop()
  }

  "KafkaSink.runStream" should {
    "throw an exception" in {
      val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
      withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
        val result = KafkaSink.runStream("", "", "", "", Earliest)
        result.left.get shouldBe a[Throwable]
      }
    }
    EmbeddedKafka.stop()
  }
}
