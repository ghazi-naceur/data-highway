package gn.oss.data.highway.utils

import com.typesafe.scalalogging.LazyLogging
import gn.oss.data.highway.engine.ops.KafkaAdminOps
import gn.oss.data.highway.models.DataHighwayRuntimeException.KafkaTopicNotFoundError
import gn.oss.data.highway.models.{DataHighwayError, DataHighwayErrorResponse}

object KafkaUtils extends LazyLogging {

  /**
    * Verifies a certain topic already exist, otherwise it will return an error
    *
    * @param topic The provided topic
    * @param brokerUrls The Kafka brokers urls
    * @return String, otherwise a DataHighwayErrorResponse
    */
  def verifyTopicExistence(topic: String, brokerUrls: String): Either[DataHighwayErrorResponse, String] = {
    KafkaAdminOps.listTopics(brokerUrls) match {
      case Right(topics) =>
        if (topics.contains(topic))
          Right(topic)
        else
          Left(KafkaTopicNotFoundError)
      case Left(thr) => Left(DataHighwayError(thr.getMessage, thr.getCause.toString))
    }
  }
}
