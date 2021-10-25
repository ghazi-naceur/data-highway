package gn.oss.data.highway.configs

import gn.oss.data.highway.models.LogLevel
import org.apache.spark.sql.SparkSession
import pureconfig.generic.auto._

case class SparkConfigs(appName: String, masterUrl: String, logLevel: LogLevel)

trait SparkUtils extends CassandraUtils {
  val sparkConf: SparkConfigs = ConfigLoader().loadConfigs[SparkConfigs]("spark")
  val sparkSession: SparkSession = {
    val ss = SparkSession
      .builder()
      .appName(sparkConf.appName)
      .master(sparkConf.masterUrl)
      .getOrCreate()
    ss.sparkContext.setLogLevel(sparkConf.logLevel.value)
    ss.conf.set("spark.cassandra.connection.host", cassandraConf.host)
    ss.conf.set("spark.cassandra.connection.port", cassandraConf.port)
    ss
  }
}
