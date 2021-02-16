package io.oss.data.highway.model

sealed trait KafkaMode

case class SparkKafkaPluginConsumer(offset: Offset) extends KafkaMode

case class SparkKafkaPluginStreamsConsumer(offset: Offset) extends KafkaMode

case object SparkKafkaPluginProducer extends KafkaMode

case class PureKafkaConsumer(consumerGroup: String, offset: Offset)
    extends KafkaMode

case class PureKafkaStreamsConsumer(streamAppId: String, offset: Offset)
    extends KafkaMode

case object PureKafkaProducer extends KafkaMode

case class SparkKafkaPluginStreamsProducer(offset: Offset) extends KafkaMode

case class PureKafkaStreamsProducer(streamAppId: String, offset: Offset)
    extends KafkaMode
