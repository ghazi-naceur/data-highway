package io.oss.data.highway.utils

import io.oss.data.highway.configuration.SparkConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import cats.syntax.either._

case class HdfsUtils(hadoopConf: Configuration) {

  val fs = FileSystem.get(hadoopConf)

  def save(value: String, path: String) = {
    Either.catchNonFatal {}
  }
}
