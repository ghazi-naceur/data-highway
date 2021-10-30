package gn.oss.data.highway.models

// todo to be fused with DHE
object DataHighwayErrorObj {

  case class DataHighwayFileError(
      message: String,
      cause: Throwable,
      stacktrace: Array[StackTraceElement]
  ) {
    override def toString: String =
      s"- Message: $message \n- Cause: $cause \n- Stacktrace: ${stacktrace.mkString("\n")}"
  }

  case class KafkaError(message: String, cause: Throwable, stacktrace: Array[StackTraceElement]) {
    override def toString: String =
      s"- Message: $message \n- Cause: $cause \n- Stacktrace: ${stacktrace.mkString("\n")}"
  }

  case class HdfsError(message: String, cause: Throwable, stacktrace: Array[StackTraceElement]) {
    override def toString: String =
      s"- Message: $message \n- Cause: $cause \n- Stacktrace: ${stacktrace.mkString("\n")}"
  }
}
