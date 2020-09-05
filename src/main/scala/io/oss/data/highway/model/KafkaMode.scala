package io.oss.data.highway.model

sealed trait KafkaMode

case class SimpleProducer(useConsumer: Boolean,
                          offset: Offset,
                          consumerGroup: String)
    extends KafkaMode

case class KafkaStreaming(streamAppId: String,
                          useConsumer: Boolean,
                          offset: Offset,
                          consumerGroup: String)
    extends KafkaMode

case class SparkKafkaPlugin(useStream: Boolean,
                            intermediateTopic: String,
                            checkpointFolder: String)
    extends KafkaMode
// TODO It is recommended that intermediateTopic and checkpointFolder should be changed together :
//  see : https://stackoverflow.com/questions/57030933/spark-streaming-failing-due-to-error-on-a-different-kafka-topic-than-the-one-bei
