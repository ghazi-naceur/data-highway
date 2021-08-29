package gn.oss.data.highway.models

import pureconfig.error.ConfigReaderFailures

trait DataHighwayError extends Throwable {
  val message: String
  val cause: Throwable
  val stacktrace: Array[StackTraceElement]
}

object DataHighwayError {

  case class DataHighwayFileError(
      message: String,
      cause: Throwable,
      stacktrace: Array[StackTraceElement]
  ) extends DataHighwayError {
    override def toString: String =
      s"- Message: $message \n- Cause: $cause \n- Stacktrace: ${stacktrace.mkString("\n")}"
  }

  case class PathNotFound(path: String) extends DataHighwayError {
    override val message: String                      = ""
    override val cause: Throwable                     = null
    override val stacktrace: Array[StackTraceElement] = null

    override def toString: String = s"The provided path '$path' does not exist."
  }

  case class ParquetError(message: String, cause: Throwable, stacktrace: Array[StackTraceElement])
      extends DataHighwayError {
    override def toString: String =
      s"- Message: $message \n- Cause: $cause \n- Stacktrace: ${stacktrace.mkString("\n")}"
  }

  case class CsvError(message: String, cause: Throwable, stacktrace: Array[StackTraceElement])
      extends DataHighwayError {
    override def toString: String =
      s"- Message: $message \n- Cause: $cause \n- Stacktrace: ${stacktrace.mkString("\n")}"
  }

  case class JsonError(message: String, cause: Throwable, stacktrace: Array[StackTraceElement])
      extends DataHighwayError {
    override def toString: String =
      s"- Message: $message \n- Cause: $cause \n- Stacktrace: ${stacktrace.mkString("\n")}"
  }

  case class BulkErrorAccumulator(errors: ConfigReaderFailures) extends DataHighwayError {
    override val message: String                      = ""
    override val cause: Throwable                     = null
    override val stacktrace: Array[StackTraceElement] = null
    override def toString: String =
      s"- Errors: ${errors.toList.mkString("\n")}"

  }

  case class KafkaError(message: String, cause: Throwable, stacktrace: Array[StackTraceElement])
      extends DataHighwayError {
    override def toString: String =
      s"- Message: $message \n- Cause: $cause \n- Stacktrace: ${stacktrace.mkString("\n")}"
  }

  case class AvroError(message: String, cause: Throwable, stacktrace: Array[StackTraceElement])
      extends DataHighwayError {
    override def toString: String =
      s"- Message: $message \n- Cause: $cause \n- Stacktrace: ${stacktrace.mkString("\n")}"
  }

  case class HdfsError(message: String, cause: Throwable, stacktrace: Array[StackTraceElement])
      extends DataHighwayError {
    override def toString: String =
      s"- Message: $message \n- Cause: $cause \n- Stacktrace: ${stacktrace.mkString("\n")}"
  }
}
