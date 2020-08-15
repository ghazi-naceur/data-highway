package io.oss.data.highway.model

sealed trait KafkaMode

case object ProducerConsumer extends KafkaMode
case class KafkaStreaming(outStreams: String) extends KafkaMode
case object SparkKafkaPlugin extends KafkaMode
