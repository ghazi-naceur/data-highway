package io.oss.data.highway.model

sealed trait KafkaMode {
  val brokers: String
}

case class SparkKafkaPluginConsumer(brokers: String,
                                    offset: Offset,
                                    dataType: Option[DataType])
    extends KafkaMode

case class SparkKafkaPluginStreamsConsumer(brokers: String,
                                           offset: Offset,
                                           dataType: Option[DataType])
    extends KafkaMode

case class SparkKafkaPluginProducer(brokers: String) extends KafkaMode

case class PureKafkaConsumer(brokers: String,
                             consumerGroup: String,
                             offset: Offset,
                             dataType: Option[DataType])
    extends KafkaMode

case class PureKafkaStreamsConsumer(brokers: String,
                                    streamAppId: String,
                                    offset: Offset,
                                    dataType: Option[DataType])
    extends KafkaMode

case class PureKafkaProducer(brokers: String) extends KafkaMode

case class SparkKafkaPluginStreamsProducer(brokers: String, offset: Offset)
    extends KafkaMode

case class PureKafkaStreamsProducer(brokers: String,
                                    streamAppId: String,
                                    offset: Offset)
    extends KafkaMode
