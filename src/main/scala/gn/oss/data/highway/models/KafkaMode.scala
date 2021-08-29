package gn.oss.data.highway.models

sealed trait KafkaMode {
  val brokers: String
}

sealed trait KafkaConsumer extends KafkaMode {
  val brokers: String
  val offset: Offset
}

sealed trait KafkaProducer extends KafkaMode {
  val brokers: String
}

case class SparkKafkaPluginConsumer(brokers: String, offset: Offset) extends KafkaConsumer

case class SparkKafkaPluginStreamsConsumer(brokers: String, offset: Offset) extends KafkaConsumer

case class PureKafkaStreamsConsumer(
    brokers: String,
    streamAppId: String,
    offset: Offset
) extends KafkaConsumer

case class PureKafkaConsumer(
    brokers: String,
    consumerGroup: String,
    offset: Offset
) extends KafkaConsumer

case class SparkKafkaPluginProducer(brokers: String) extends KafkaProducer

case class PureKafkaProducer(brokers: String) extends KafkaProducer

case class SparkKafkaPluginStreamsProducer(
    brokers: String,
    offset: Offset
) extends KafkaProducer

case class PureKafkaStreamsProducer(
    brokers: String,
    streamAppId: String,
    offset: Offset
) extends KafkaProducer
