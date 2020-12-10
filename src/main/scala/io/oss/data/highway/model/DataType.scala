package io.oss.data.highway.model

sealed trait DataType {
  val extension: String
}

case object JSON extends DataType {
  override val extension: String = "json"
}
case object CSV extends DataType {
  override val extension: String = "csv"
}
case object PARQUET extends DataType {
  override val extension: String = "parquet"
}
case object AVRO extends DataType {
  override val extension: String = "avro"
}
case object XLSX extends DataType {
  override val extension: String = "xls"
}
case object XLS extends DataType {
  override val extension: String = "xlsx"
}
