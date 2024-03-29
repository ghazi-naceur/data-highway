package gn.oss.data.highway.models

sealed trait DataType {
  val extension: String
}

case object JSON extends DataType {
  override val extension: String = "json"
}
case class CSV(inferSchema: Boolean, header: Boolean, separator: String) extends DataType {
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
case class XML(rootTag: String, rowTag: String) extends DataType {
  override val extension: String = "xml"
}
case class CassandraDB(keyspace: String, table: String) extends DataType {
  override val extension: String = ""
}
case class PostgresDB(database: String, table: String) extends DataType {
  override val extension: String = ""
}

sealed trait Compression {
  val value: String
}

sealed trait OrcCompression extends Compression
sealed trait ParquetCompression extends Compression

case object Lzo extends OrcCompression {
  override val value: String = "lzo"
}
case object Zlib extends OrcCompression {
  override val value: String = "zlib"
}
case object Gzip extends ParquetCompression {
  override val value: String = "gzip"
}
case object Snappy extends OrcCompression with ParquetCompression {
  override val value: String = "snappy"
}
case object None extends OrcCompression with ParquetCompression {
  override val value: String = "none"
}
