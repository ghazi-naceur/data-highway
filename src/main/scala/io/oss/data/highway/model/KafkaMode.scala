package io.oss.data.highway.model

sealed trait KafkaMode

case class ProducerConsumer(useConsumer: Boolean,
                            offset: Offset,
                            consumerGroup: String)
    extends KafkaMode
case class KafkaStreaming(outStreams: String,
                          useConsumer: Boolean,
                          offset: Offset,
                          consumerGroup: String)
    extends KafkaMode
case object SparkKafkaPlugin extends KafkaMode
