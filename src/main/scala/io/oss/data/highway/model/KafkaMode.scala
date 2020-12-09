package io.oss.data.highway.model

sealed trait KafkaMode

case class SparkKafkaConsumerPlugin(useStream: Boolean) extends KafkaMode

case class SparkKafkaProducerPlugin(useStream: Boolean) extends KafkaMode

case class PureKafkaProducer(useStream: Boolean, streamAppId: Option[String])
    extends KafkaMode

object PureKafkaProducer {
  def apply(useStream: Boolean,
            streamAppId: Option[String]): PureKafkaProducer =
    if (useStream) {
      if (streamAppId.isEmpty)
        throw new RuntimeException(
          "Must set 'stream-app-id' field when using streaming mode for pure kafka producer.")
      else new PureKafkaProducer(useStream, streamAppId)
    } else {
      new PureKafkaProducer(useStream, streamAppId)
    }
}

case class PureKafkaConsumer(useStream: Boolean, streamAppId: Option[String])
    extends KafkaMode
object PureKafkaConsumer {
  def apply(useStream: Boolean,
            streamAppId: Option[String]): PureKafkaConsumer =
    if (useStream) {
      if (streamAppId.isEmpty)
        throw new RuntimeException(
          "Must set 'stream-app-id' field when using streaming mode for pure kafka consumer.")
      else new PureKafkaConsumer(useStream, streamAppId)
    } else {
      new PureKafkaConsumer(useStream, streamAppId)
    }
}
