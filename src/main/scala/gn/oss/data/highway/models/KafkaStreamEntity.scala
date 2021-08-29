package gn.oss.data.highway.models

import java.util.Properties

import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream

case class KafkaStreamEntity(
    props: Properties,
    builder: StreamsBuilder,
    dataKStream: KStream[String, String]
)
