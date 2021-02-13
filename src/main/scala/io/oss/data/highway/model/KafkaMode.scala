package io.oss.data.highway.model

sealed trait KafkaMode

case object SparkKafkaPluginConsumer extends KafkaMode

case object SparkKafkaPluginStreamsConsumer extends KafkaMode

case object SparkKafkaPluginProducer extends KafkaMode

case object PureKafkaConsumer extends KafkaMode

case class PureKafkaStreamsConsumer(streamAppId: String) extends KafkaMode

case object PureKafkaProducer extends KafkaMode

case class SparkKafkaPluginStreamsProducer(offset: Offset) extends KafkaMode

case class PureKafkaStreamsProducer(streamAppId: String, offset: Offset)
    extends KafkaMode
