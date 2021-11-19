package gn.oss.data.highway.models

sealed trait RequestType {
  val param: String
}

case object RouteRequest extends RequestType {
  override val param = "route"
}
case object QueryRequest extends RequestType {
  override val param = "query"
}
