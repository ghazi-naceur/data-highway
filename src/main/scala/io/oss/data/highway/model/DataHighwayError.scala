package io.oss.data.highway.model

trait DataHighwayError extends Throwable {
  val message: String
  val cause: Throwable
  val stacktrace: Array[StackTraceElement]

  def asString: String
}

object DataHighwayError {

  case class ReadFileError(message: String,
                           cause: Throwable,
                           stacktrace: Array[StackTraceElement])
      extends DataHighwayError {
    override def asString: String =
      s"- Message: $message \n- Cause: $cause \n- Stacktrace: ${stacktrace.mkString("\n")}"
  }

  case class PathNotFound(path: String) extends DataHighwayError {
    override val message: String = ""
    override val cause: Throwable = null
    override val stacktrace: Array[StackTraceElement] = null

    override def asString: String = s"The provided path '$path' does not exist."
  }

  case class ParquetError(message: String,
                          cause: Throwable,
                          stacktrace: Array[StackTraceElement])
      extends DataHighwayError {
    override def asString: String =
      s"- Message: $message \n- Cause: $cause \n- Stacktrace: ${stacktrace.mkString("\n")}"
  }

  case class CsvError(message: String,
                      cause: Throwable,
                      stacktrace: Array[StackTraceElement])
      extends DataHighwayError {
    override def asString: String =
      s"- Message: $message \n- Cause: $cause \n- Stacktrace: ${stacktrace.mkString("\n")}"
  }

}
