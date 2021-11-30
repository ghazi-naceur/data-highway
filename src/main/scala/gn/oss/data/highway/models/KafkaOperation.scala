package gn.oss.data.highway.models

sealed trait KafkaOperation

case class TopicCreation(topicName: String, brokerHosts: String, partitions: Int, replicationFactor: Short)
    extends KafkaOperation
case class TopicDeletion(topicName: String, brokerHosts: String) extends KafkaOperation
case class TopicsList(brokerHosts: String) extends KafkaOperation
