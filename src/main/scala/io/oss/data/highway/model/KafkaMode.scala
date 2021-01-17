package io.oss.data.highway.model

sealed trait KafkaMode

case class SparkKafkaConsumerPlugin(useStream: Boolean) extends KafkaMode

case object SparkKafkaPluginProducer extends KafkaMode

case object PureKafkaConsumer extends KafkaMode

case class PureKafkaStreamsConsumer(streamAppId: String) extends KafkaMode

case object PureKafkaProducer extends KafkaMode

case object SparkKafkaPluginStreamsProducer extends KafkaMode

case class PureKafkaStreamsProducer(streamAppId: String) extends KafkaMode
