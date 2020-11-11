package io.oss.data.highway.model

sealed trait KafkaMode

case object SimpleProducer extends KafkaMode

case object SimpleConsumer extends KafkaMode

case class KafkaStreaming(streamAppId: String) extends KafkaMode

case class SparkKafkaConsumerPlugin(useStream: Boolean) extends KafkaMode

// todo The intermediateTopic must be pre-created !!!
case class SparkKafkaProducerPlugin(useStream: Boolean,
                                    intermediateTopic: String,
                                    checkpointFolder: String)
    extends KafkaMode
// TODO It is recommended that intermediateTopic and checkpointFolder should be changed together :
//  see : https://stackoverflow.com/questions/57030933/spark-streaming-failing-due-to-error-on-a-different-kafka-topic-than-the-one-bei
