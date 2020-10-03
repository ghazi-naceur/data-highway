package io.oss.data.highway.model

sealed trait Channel

case object XlsxCsv extends Channel

case object CsvParquet extends Channel

case object JsonParquet extends Channel

case object ParquetCsv extends Channel

case object JsonCsv extends Channel

case object ParquetJson extends Channel

case object CsvJson extends Channel

case object JsonKafka extends Channel

case object ParquetAvro extends Channel

case object JsonAvro extends Channel
