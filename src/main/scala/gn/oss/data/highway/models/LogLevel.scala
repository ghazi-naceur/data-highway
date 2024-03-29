package gn.oss.data.highway.models

sealed trait LogLevel {
  val value: String
}

case object INFO extends LogLevel {
  override val value: String = "INFO"
}

case object WARN extends LogLevel {
  override val value: String = "WARN"
}

case object ERROR extends LogLevel {
  override val value: String = "ERROR"
}
