package io.oss.data.highway.model

sealed trait KafkaMode

case object SimpleProducer extends KafkaMode

case object SimpleConsumer extends KafkaMode

case class KafkaStreaming(streamAppId: String) extends KafkaMode

case class SparkKafkaConsumerPlugin(useStream: Boolean) extends KafkaMode

case class SparkKafkaProducerPlugin(useStream: Boolean) extends KafkaMode
