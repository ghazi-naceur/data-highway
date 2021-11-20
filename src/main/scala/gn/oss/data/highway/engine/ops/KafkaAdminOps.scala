package gn.oss.data.highway.engine.ops

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.{Collections, Properties}
import cats.implicits._
import gn.oss.data.highway.models.DataHighwayKafkaResponse

import scala.jdk.CollectionConverters._

object KafkaAdminOps extends LazyLogging {

  /**
    * Lists the available kafka topics
    *
    * @param brokerUrls The brokers urls
    * @return a List of String, otherwise a Throwable
    */
  def listTopics(brokerUrls: String): Either[Throwable, List[String]] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrls)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "list-topics-consumer-group")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

    Either.catchNonFatal {
      val consumer = new KafkaConsumer[String, String](props)
      consumer.listTopics().asScala.toList.map(_._1)
    }
  }

  /**
    * Creates a Kafka topic
    *
    * @param topic The topic to be created
    * @param brokerUrls The kafka brokers urls
    * @return DataHighwayKafkaResponse, otherwise a Throwable
    */
  def createTopic(topic: String, brokerUrls: String): Either[Throwable, DataHighwayKafkaResponse] = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrls)
    Either.catchNonFatal {
      val adminClient = AdminClient.create(props)
      val newTopic = new NewTopic(topic, 1, 1.asInstanceOf[Short])
      val createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic))
      createTopicsResult.values().get(topic).get()
      logger.info(s"The topic '$topic' was successfully created.")
      DataHighwayKafkaResponse(topic, s"The topic '$topic' was successfully created.")
    }
  }

  /**
    * Deletes a Kafka topic
    *
    * @param topic The topic to be created
    * @param brokerUrls The kafka brokers urls
    * @return DataHighwayKafkaResponse, otherwise a Throwable
    */
  def deleteTopic(topic: String, brokerUrls: String): Either[Throwable, DataHighwayKafkaResponse] = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrls)
    Either.catchNonFatal {
      val adminClient = AdminClient.create(props)
      val deleteTopicsResult = adminClient.deleteTopics(Collections.singleton(topic))
      deleteTopicsResult.values().get(topic).get()
      logger.info(s"The topic '$topic' was successfully deleted")
      DataHighwayKafkaResponse(topic, s"The topic '$topic' was successfully deleted.")
    }
  }
}
