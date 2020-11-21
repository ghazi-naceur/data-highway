package io.oss.data.highway.utils

import java.util.{Collections, Properties}

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.log4j.Logger
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}

import scala.jdk.CollectionConverters._
import scala.util.Try

object KafkaUtils {

  val logger: Logger = Logger.getLogger(KafkaUtils.getClass.getName)

  /**
    * Lists the available kafka topics
    * @param zookeeperUrls The zookeeper urls
    * @return a List of Kafka topic names
    */
  def listTopics(zookeeperUrls: String): List[String] = {
    val watcher = new Watcher {
      override def process(event: WatchedEvent): Unit = {}
    }

    val zk: ZooKeeper = new ZooKeeper(zookeeperUrls, 100000, watcher)
    zk.getChildren("/brokers/topics", watcher).asScala.toList
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
    * @param zookeeperUrls The zookeeper urls
    * @param brokerUrls The Kafka brokers urls
    * @param enableTopicCreation The topic creation flag
    * @return Any
    */
  def verifyTopicExistence(topic: String,
                           zookeeperUrls: String,
                           brokerUrls: String,
                           enableTopicCreation: Boolean): Any = {
    if (!listTopics(zookeeperUrls).contains(topic)) {
      if (enableTopicCreation) {
        createTopic(topic, brokerUrls)
      } else {
        throw new RuntimeException(s"The topic '$topic' does not exist")
      }
    }
  }
}
