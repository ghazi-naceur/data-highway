package io.oss.data.highway.model

sealed trait KafkaMode

case object ProducerConsumer extends KafkaMode
case object KafkaStreams extends KafkaMode
case object SparkKafkaPlugin extends KafkaMode
