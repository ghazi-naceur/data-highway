package gn.oss.data.highway.utils

import java.util
import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.{Serdes, StringDeserializer}
import cats.syntax.either._
import gn.oss.data.highway.models.{KafkaStreamEntity, Offset}
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.log4j.Logger

object KafkaTopicConsumer {

  val logger: Logger = Logger.getLogger(KafkaTopicConsumer.getClass.getName)

  /**
    * Consumes from a kafka topic using a simple kafka consumer
    *
    * @param topic The input source topic
    * @param brokerUrls The kafka brokers urls
    * @param offset The consumer offset
    * @param consumerGroup The consumer group name
    * @return A KafkaConsumer, otherwise a Throwable
    */
  def consume(
    topic: String,
    brokerUrls: String,
    offset: Offset,
    consumerGroup: String
  ): Either[Throwable, KafkaConsumer[String, String]] = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrls)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset.value)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup)
    Either.catchNonFatal {
      val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
      consumer.subscribe(util.Arrays.asList(topic))
      logger.info(s"Successfully subscribing to '$topic' topic.")
      consumer
    }
  }

  /**
    * Consumes from a kafka topic using kafka streams
    *
    * @param streamAppId The stream application id
    * @param topic The input source topic
    * @param offset The consumer offset
    * @param bootstrapServers THe kafka brokers urls
    * @return KafkaStreamEntity
    */
  def consumeWithStream(
    streamAppId: String,
    topic: String,
    offset: Offset,
    bootstrapServers: String
  ): KafkaStreamEntity = {
    import org.apache.kafka.streams.scala.ImplicitConversions._
    import org.apache.kafka.streams.scala.Serdes._

    val props = new Properties
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, streamAppId)
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset.value)

    val builder = new StreamsBuilder

    val dataKStream = builder.stream[String, String](topic)
    logger.info(s"Successfully creating KStream for '$topic' topic.")
    KafkaStreamEntity(props, builder, dataKStream)
  }
}
