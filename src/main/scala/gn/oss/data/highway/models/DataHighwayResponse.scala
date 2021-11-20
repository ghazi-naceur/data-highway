package gn.oss.data.highway.models

import pureconfig.error.ConfigReaderFailures

sealed trait DataHighwayResponse

sealed trait DataHighwaySuccessResponse
sealed trait DataHighwayErrorResponse extends Throwable

case class DataHighwaySuccess(input: String, output: String) extends DataHighwaySuccessResponse

case class DataHighwayElasticResponse(index: String, description: String) extends DataHighwaySuccessResponse

case class DataHighwayKafkaResponse(topic: String, description: String) extends DataHighwaySuccessResponse

case class DataHighwayError(message: String, cause: String) extends DataHighwayErrorResponse
object DataHighwayError {
  def prettyError(thr: Throwable): String =
    s"""
       | - Message: ${thr.getMessage}
       | - Cause: ${thr.getCause}
       | - Stacktrace: ${thr.getStackTrace.toList.mkString("\n")}
       |""".stripMargin
  def prettyErrors(errors: ConfigReaderFailures): String = errors.toList.mkString("\n")
}

sealed trait DataHighwayRuntimeException extends DataHighwayErrorResponse
object DataHighwayRuntimeException {
  val MustNotHaveSaveModeError: DataHighwayErrorResponse =
    DataHighwayError(
      "The 'Consistency' property must not be set.",
      "This route should not have a 'consistency' field, which represents the 'SaveMode', " +
        "because it uses an implicit one. This route should handle 'Kafka' or 'Elasticsearch' as an output."
    )
  val MustHaveSaveModeError: DataHighwayErrorResponse =
    DataHighwayError(
      "The 'Consistency' property must be set.",
      "This route should have a 'consistency' field, which represents the 'SaveMode'. " +
        "It should handle 'File', 'Postgres' or 'Cassandra' as an output."
    )
  val MustHaveFileSystemError: DataHighwayErrorResponse =
    DataHighwayError(
      "The 'Storage' property must be set.",
      "This route should have a 'storage' field, which represents the 'FileSystem'."
    )
  val MustHaveFileSystemAndSaveModeError: DataHighwayErrorResponse =
    DataHighwayError(
      "The 'Storage' and 'Consistency' properties must be set.",
      "This route should have a 'storage' and 'consistency' fields, which represents respectively the 'FileSystem' and the 'SaveMode'."
    )
  val MustHaveSearchQueryError: DataHighwayErrorResponse =
    DataHighwayError("The 'SearchQuery' property must be set.", "This route should have a 'search-query' field.")
  val KafkaProducerSupportModeError: DataHighwayErrorResponse =
    DataHighwayError(
      "This mode is not supported while publishing files' content to Kafka topic.",
      s"The supported modes are ${PureKafkaProducer.getClass} and ${SparkKafkaPluginProducer.getClass}."
    )
  val KafkaMirrorSupportModeError: DataHighwayErrorResponse =
    DataHighwayError(
      "This mode is not supported while mirroring kafka topics.",
      s"The supported modes are ${PureKafkaStreamsProducer.getClass} and ${SparkKafkaPluginStreamsProducer.getClass}."
    )
  val KafkaConsumerSupportModeError: DataHighwayErrorResponse =
    DataHighwayError(
      "This mode is not supported while consuming Kafka topics.",
      s"The supported modes are ${PureKafkaConsumer.getClass}, ${SparkKafkaPluginConsumer.getClass} and ${PureKafkaStreamsConsumer.getClass}."
    )
  val KafkaConsumerMissingModeError: DataHighwayErrorResponse =
    DataHighwayError(
      "This mode needs a Kafka consumer mode.",
      s"The supported modes are ${PureKafkaConsumer.getClass}, ${SparkKafkaPluginConsumer.getClass} and ${PureKafkaStreamsConsumer.getClass}."
    )
  val RouteError: DataHighwayErrorResponse = {
    DataHighwayError(
      "The provided route is not supported yet. ",
      "This route will be implemented in the upcoming versions. For now, you can combine all the available routes to " +
        "ensure sending data to your desired destination."
    )
  }
  val KafkaTopicNotFoundError: DataHighwayErrorResponse = {
    DataHighwayError(s"The topic does not exist.", "The provided topic is not present. You may want to create it first.")
  }
}
