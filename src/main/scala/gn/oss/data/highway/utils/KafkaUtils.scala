package gn.oss.data.highway.utils

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import gn.oss.data.highway.engine.ops.KafkaAdminOps
import gn.oss.data.highway.models.DataHighwayError

object KafkaUtils extends LazyLogging {

  /**
    * Verifies the topic existence. If it already exists, the function will do nothing. Otherwise, in the case of
    * the activation of the 'enableTopicCreation' flag (Producing in a kafka topic), it will create the topic.
    * But in the other case where the flag is deactivated (Consuming from a kafka topic), it will throw an Exception.
    *
    * @param topic The provided topic
    * @param brokerUrls The Kafka brokers urls
    * @param enableTopicCreation The topic creation flag
    * @return Any, otherwise a Throwable
    */
  def verifyTopicExistence(topic: String, brokerUrls: String, enableTopicCreation: Boolean): Either[Throwable, Any] = {
    Either.catchNonFatal {
      val topics = KafkaAdminOps.listTopics(brokerUrls)
      if (!topics.contains(topic)) {
        if (enableTopicCreation) {
          KafkaAdminOps.createTopic(topic, brokerUrls)
        } else {
          Left(DataHighwayError(s"The topic '$topic' does not exist.", Constants.EMPTY))
        }
      }
    }
  }
}
