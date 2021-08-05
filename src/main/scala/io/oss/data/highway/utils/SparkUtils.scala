package io.oss.data.highway.utils

import io.oss.data.highway.configs.{CassandraConfigs, ConfigLoader, SparkConfigs}
import org.apache.spark.sql.SparkSession

trait SparkUtils {
  val sparkConf: SparkConfigs         = ConfigLoader().loadSparkConf()
  val cassandraConf: CassandraConfigs = ConfigLoader().loadCassandraConf()
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
