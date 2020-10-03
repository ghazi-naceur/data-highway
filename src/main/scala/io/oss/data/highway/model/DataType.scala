package io.oss.data.highway.model

sealed trait DataType

case object JSON extends DataType
case object CSV extends DataType
case object PARQUET extends DataType
case object AVRO extends DataType
