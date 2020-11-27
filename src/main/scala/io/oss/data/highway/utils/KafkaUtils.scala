package io.oss.data.highway.utils

import java.util
import java.util.{Collections, Properties}

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.PartitionInfo
import org.apache.log4j.Logger

import scala.jdk.CollectionConverters._
import scala.util.Try

object KafkaUtils {

  val logger: Logger = Logger.getLogger(KafkaUtils.getClass.getName)

  /**
    * Lists the available kafka topics
    * @param brokerUrls The brokers urls
    * @return a List of Kafka topic names
    */
  def listTopics(
      brokerUrls: String): List[(String, util.List[PartitionInfo])] = {
    val props = new Properties()
    props.put("bootstrap.servers", brokerUrls);
    props.put("group.id", "list-topics-consumer-group");
    props.put("key.deserializer",
              "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer",
              "org.apache.kafka.common.serialization.StringDeserializer");

    val consumer = new KafkaConsumer[String, String](props)
    consumer.listTopics().asScala.toList
  }

  /**
    * Creates a Kafka topic
    * @param topic The topic to be created
    * @param brokerUrls The kafka brokers urls
    * @return Unit, otherwise a Throwable
    */
  private def createTopic(topic: String,
                          brokerUrls: String): Either[Throwable, Unit] = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrls)
    Try {
      val adminClient = AdminClient.create(props)
      val newTopic = new NewTopic(topic, 1, 1.asInstanceOf[Short])
      val createTopicsResult =
        adminClient.createTopics(Collections.singleton(newTopic))
      createTopicsResult.values().get(topic).get()
      logger.info(s"The topic '$topic' was successfully created")
    }.toEither
  }

  /**
    * Verifies the topic existence. If it already exists, the function will do nothing. Otherwise, in the case of
    * the activation of the 'enableTopicCreation' flag (Producing in a kafka topic), it will create the topic.
    * But in the other case where the flag is deactivated (Consuming from a kafka topic), it will throw an Exception.
    * @param topic The provided topic
    * @param brokerUrls The Kafka brokers urls
    * @param enableTopicCreation The topic creation flag
    * @return Any
    */
  def verifyTopicExistence(topic: String,
                           brokerUrls: String,
                           enableTopicCreation: Boolean): Any = {
    val strings = listTopics(brokerUrls).map(_._1)
    if (!strings.contains(topic)) {
      if (enableTopicCreation) {
        createTopic(topic, brokerUrls)
      } else {
        throw new RuntimeException(s"The topic '$topic' does not exist")
      }
    }
  }
}
