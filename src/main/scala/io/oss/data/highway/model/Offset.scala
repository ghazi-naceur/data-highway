package io.oss.data.highway.model

sealed trait Offset {
  val value: String
}

case object Latest extends Offset {
  override val value: String = "latest"
}

case object Earliest extends Offset {
  override val value: String = "earliest"
}
