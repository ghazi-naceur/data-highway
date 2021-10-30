package gn.oss.data.highway.models

sealed trait DataHighwayResponse

sealed trait DataHighwaySuccessResponse
sealed trait DataHighwayErrorResponse extends Throwable

case class DataHighwaySuccess(input: String, output: String) extends DataHighwaySuccessResponse

case class DataHighwayElasticResponse(index: String, description: String)
    extends DataHighwaySuccessResponse

case class DataHighwayError(message: String, cause: String) extends DataHighwayErrorResponse

sealed trait DataHighwayRuntimeException extends DataHighwayErrorResponse
object DataHighwayRuntimeException {
  val MustNotHaveExplicitSaveModeError: DataHighwayErrorResponse =
    DataHighwayError(
      "The 'Consistency' property must not be set.",
      "This route should not have an explicit 'consistency' field, which represents the 'SaveMode', " +
        "because it uses an implicit one. This route should handle 'Kafka' or 'Elasticsearch' as an output."
    )
  val MustHaveExplicitSaveModeError: DataHighwayErrorResponse =
    DataHighwayError(
      "The 'Consistency' property must be set.",
      "This route should have an explicit 'consistency' field, which represents the 'SaveMode'. " +
        "It should handle 'File', 'Postgres' or 'Cassandra' as an output."
    )
  val MustHaveExplicitFileSystemError: DataHighwayErrorResponse =
    DataHighwayError(
      "The 'Storage' property must be set.",
      "This route should have an explicit 'storage' field, which represents the 'FileSystem'."
    )
  val MustHaveFileSystemAndSaveModeError: DataHighwayErrorResponse =
    DataHighwayError(
      "The 'Storage' and 'Consistency' properties must be set.",
      "This route should have a 'storage' and 'consistency' fields, which represents respectively the 'FileSystem' and the 'SaveMode'."
    )
  val MustHaveSearchQueryError: DataHighwayErrorResponse =
    DataHighwayError(
      "The 'SearchQuery' property must be set.",
      "This route should have an explicit 'search-query' field."
    )
}
