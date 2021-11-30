package gn.oss.data.highway.engine.ops

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.{Collections, Properties}
import cats.implicits._
import gn.oss.data.highway.models.{
  TopicCreation,
  DataHighwayError,
  DataHighwayErrorResponse,
  DataHighwayKafkaResponse,
  DataHighwayKafkaTopicsListResponse,
  DataHighwaySuccessResponse,
  TopicDeletion,
  KafkaOperation,
  TopicsList
}
import gn.oss.data.highway.utils.Constants.EMPTY

import scala.jdk.CollectionConverters._

object KafkaAdminOps extends LazyLogging {

  /**
    * Executes an Kafka Ops operation
    *
    * @param operation THe Kafka operation
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  def execute(operation: KafkaOperation): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    operation match {
      case TopicCreation(topicName, brokerHosts, partitions, replicationFactor) =>
        createTopic(topicName, brokerHosts, partitions, replicationFactor)
      case TopicDeletion(topicName, brokerHosts) => deleteTopic(topicName, brokerHosts)
      case TopicsList(brokerHosts)               => listTopics(brokerHosts)
    }
  }

  /**
    * Creates a Kafka topic
    *
    * @param topicName The topic to be created
    * @param brokerHosts The kafka brokers hosts
    * @param partitions The kafka topic partitions number
    * @param replicationFactor The kafka topic replication factor
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  def createTopic(
    topicName: String,
    brokerHosts: String,
    partitions: Int,
    replicationFactor: Short
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerHosts)
    Either.catchNonFatal {
      val adminClient = AdminClient.create(props)
      val newTopic = new NewTopic(topicName, partitions, replicationFactor)
      val createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic))
      createTopicsResult.values().get(topicName).get()
      logger.info(s"The topic '$topicName' was successfully created.")
      DataHighwayKafkaResponse(topicName, s"The topic '$topicName' was successfully created.")
    }.leftMap(thr => {
      logger.error(DataHighwayError.prettyError(thr))
      DataHighwayError(thr.getMessage, EMPTY)
    })
  }

  /**
    * Deletes a Kafka topic
    *
    * @param topic The topic to be created
    * @param brokerUrls The kafka brokers urls
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  def deleteTopic(topic: String, brokerUrls: String): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrls)
    Either.catchNonFatal {
      val adminClient = AdminClient.create(props)
      val deleteTopicsResult = adminClient.deleteTopics(Collections.singleton(topic))
      deleteTopicsResult.values().get(topic).get()
      logger.info(s"The topic '$topic' was successfully deleted")
      DataHighwayKafkaResponse(topic, s"The topic '$topic' was successfully deleted.")
    }.leftMap(thr => {
      logger.error(DataHighwayError.prettyError(thr))
      DataHighwayError(thr.getMessage, EMPTY)
    })
  }

  /**
    * Lists the available kafka topics
    *
    * @param brokerUrls The brokers urls
    * @return a List of String, otherwise a Throwable
    */
  def listTopics(brokerUrls: String): Either[DataHighwayErrorResponse, DataHighwayKafkaTopicsListResponse] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrls)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "list-topics-consumer-group")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

    Either.catchNonFatal {
      val consumer = new KafkaConsumer[String, String](props)
      val topics = consumer.listTopics().asScala.toList.map(_._1)
      DataHighwayKafkaTopicsListResponse(topics, "All available Kafka topics retrieved successfully")
    }.leftMap(thr => {
      logger.error(DataHighwayError.prettyError(thr))
      DataHighwayError(thr.getMessage, EMPTY)
    })
  }

}
