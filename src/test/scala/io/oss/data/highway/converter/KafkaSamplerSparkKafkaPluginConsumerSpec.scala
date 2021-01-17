package io.oss.data.highway.converter

import io.oss.data.highway.configuration.SparkConfigs
import io.oss.data.highway.model.{
  Earliest,
  INFO,
  JSON,
  SparkKafkaPluginConsumer
}
import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.wordspec.AnyWordSpec

import java.io.File
import java.nio.file.{Files, Paths}
import scala.io.Source
import scala.reflect.io.Directory

class KafkaSamplerSparkKafkaPluginConsumerSpec
    extends AnyWordSpec
    with EmbeddedKafka {
//  In-memory Zookeeper and Kafka will be instantiated respectively on port 6000 and 6001 and automatically shutdown at the end of the test.
  val out = "/tmp/src/test/resources/output/files"
  val brokerUrl = "localhost:6001"
  val sparkConfig: SparkConfigs = SparkConfigs("app-name", "local[*]", INFO)

  private def deleteFolderWithItsContent(path: String): Unit = {
    new File(path).listFiles.toList
      .foreach(file => {
        val path = Paths.get(file.getPath)
        val directory = new Directory(file)
        directory.deleteRecursively()
        Files.deleteIfExists(path)
      })
  }

  "runs with embedded kafka" should {
    "work using Spark Kafka consumer plugin" in {
      withRunningKafka {
        publishStringMessageToKafka("topic1",
                                    "{\"something\": \"something else\"}")
        KafkaSampler.consumeFromTopic("topic1",
                                      out,
                                      Some(JSON),
                                      SparkKafkaPluginConsumer,
                                      brokerUrl,
                                      Earliest,
                                      "cons",
                                      sparkConfig)
        val generatedFile = new File(out).listFiles().head
        val buffer = Source.fromFile(generatedFile)
        val content = buffer.getLines.toList.head
        buffer.close()
        assert(
          content == "[{\"value\":\"{\\\"something\\\": \\\"something else\\\"}\"}]")
        deleteFolderWithItsContent(out)
      }
    }
  }
}
