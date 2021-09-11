package gn.oss.data.highway.models

sealed trait DataType {
  val extension: String
}

case object JSON extends DataType {
  override val extension: String = "json"
}
case object CSV extends DataType {
  override val extension: String = "csv"
}
case class PARQUET(compression: Option[ParquetCompression]) extends DataType {
  override val extension: String = "parquet"
}
case object AVRO extends DataType {
  override val extension: String = "avro"
}
case object XLSX extends DataType {
  override val extension: String = "xlsx"
}
case class ORC(compression: Option[OrcCompression]) extends DataType {
  override val extension: String = "orc"
}
case class CassandraDB(keyspace: String, table: String) extends DataType {
  override val extension: String = ""
}

sealed trait OrcCompression {
  val value: String
}
case object Lzo extends OrcCompression {
  override val value: String = "lzo"
}
case object Snappy extends OrcCompression with ParquetCompression {
  override val value: String = "snappy"
}
case object Zlib extends OrcCompression {
  override val value: String = "zlib"
}
case object None extends OrcCompression with ParquetCompression {
  override val value: String = "none"
}

sealed trait ParquetCompression {
  val value: String
}
case object Gzip extends ParquetCompression {
  override val value: String = "gzip"
}
