package gn.oss.data.highway.models

sealed trait DataHighwayResponse

sealed trait DataHighwaySuccessResponse
sealed trait DataHighwayErrorResponse {
  def toThrowable: Throwable
}

case class DataHighwayIOResponse(input: String, output: String, description: String)
    extends DataHighwaySuccessResponse

case class DataHighwayElasticResponse(index: String, description: String)
    extends DataHighwaySuccessResponse

case class DHErrorResponse(message: String, cause: String, description: String)
    extends DataHighwayErrorResponse {
  def toThrowable: Throwable = new RuntimeException(s"$cause == $message")
}
