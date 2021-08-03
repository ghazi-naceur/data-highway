package io.oss.data.highway.utils

import io.oss.data.highway.configs.{ConfigLoader, SparkConfigs}
import org.apache.spark.sql.SparkSession

trait SparkUtils {

  val sparkConf: SparkConfigs = ConfigLoader().loadSparkConf()
  val sparkSession: SparkSession = {
    val ss = SparkSession
      .builder()
      .appName(sparkConf.appName)
      .master(sparkConf.masterUrl)
      .getOrCreate()
    ss.sparkContext.setLogLevel(sparkConf.logLevel.value)
    ss.conf
      .set(s"spark.sql.catalog.cass100", "com.datastax.spark.connector.datasource.CassandraCatalog")
    ss.conf.set(s"spark.sql.catalog.cass100.spark.cassandra.connection.host", "127.0.0.100")
    ss
  }
}
