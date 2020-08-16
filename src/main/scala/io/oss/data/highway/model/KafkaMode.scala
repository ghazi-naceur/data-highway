package io.oss.data.highway.model

sealed trait KafkaMode

case class ProducerConsumer(useConsumer: Boolean,
                            offset: Offset,
                            consumerGroup: String)
    extends KafkaMode
// TODO Use an intermediate topic and remove out-streams from config + keep "out" as a the output topic
case class KafkaStreaming(outStreams: String,
                          useConsumer: Boolean,
                          offset: Offset,
                          consumerGroup: String)
    extends KafkaMode
case class SparkKafkaPlugin(useConsumer: Boolean,
                            offset: Offset,
                            consumerGroup: String,
                            useStream: Boolean)
    extends KafkaMode
