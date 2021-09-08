package gn.oss.data.highway.models

sealed trait DataHighwayResponse

case class DataHighwayIOResponse(input: String, output: String, description: String)
    extends DataHighwayResponse

case class DataHighwayElasticResponse(index: String, description: String)
    extends DataHighwayResponse

case class DataHighwayErrorResponse(message: String, cause: String, description: String)
    extends DataHighwayResponse {
  def toThrowable: Throwable = new RuntimeException(s"$cause == $message")
}
