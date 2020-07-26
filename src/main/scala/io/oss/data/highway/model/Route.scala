package io.oss.data.highway.model

sealed trait Route

case class XlsxToCsv(in: String, out: String) extends Route
case class CsvToParquet(in: String, out: String) extends Route
case class ParquetToCsv(in: String, out: String) extends Route

case class ParquetToJson(in: String, out: String) extends Route
