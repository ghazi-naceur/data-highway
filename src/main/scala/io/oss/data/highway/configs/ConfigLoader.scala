package io.oss.data.highway.configs

import cats.syntax.either._
import io.oss.data.highway.models.DataHighwayError.BulkErrorAccumulator
import io.oss.data.highway.models.{LogLevel, Offset, Query}
import pureconfig.generic.semiauto._

case class ConfigLoader() {

  /**
    * Loads Data Highway Route configurations
    * @return a Route, otherwise a BulkErrorAccumulator
    */
  def loadConf(): Either[BulkErrorAccumulator, Query] = {
    import pureconfig._
    import pureconfig.generic.auto._ // To be kept, even though intellij didn't recognize its usage

    implicit val offsetConvert: ConfigReader[Offset] =
      deriveEnumerationReader[Offset]

    ConfigSource.default
      .at("route")
      .load[Query]
      .leftMap(thrs => BulkErrorAccumulator(thrs))
  }

  /**
    * Loads Spark configurations
    * @return SparkConfigs, otherwise throws a RuntimeException
    */
  def loadSparkConf(): SparkConfigs = {
    import pureconfig._
    import pureconfig.generic.auto._ // To be kept, even though intellij didn't recognize its usage

    implicit val offsetConvert: ConfigReader[LogLevel] =
      deriveEnumerationReader[LogLevel]

    ConfigSource.default
      .at("spark")
      .load[SparkConfigs] match {
      case Right(conf) => conf
      case Left(thr) =>
        throw new RuntimeException(
          s"Error when trying to load Spark configuration : ${thr.toList.mkString("\n")}"
        )
    }
  }

  /**
    * Loads Elasticsearch configurations
    * @return ElasticConfigs, otherwise throws a RuntimeException
    */
  def loadElasticConf(): ElasticConfigs = {
    import pureconfig._
    import pureconfig.generic.auto._ // To be kept, even though intellij didn't recognize its usage

    ConfigSource.default
      .at("elasticsearch")
      .load[ElasticConfigs] match {
      case Right(conf) => conf
      case Left(thr) =>
        throw new RuntimeException(
          s"Error when trying to load Elasticsearch configuration : ${thr.toList
            .mkString("\n")}"
        )
    }
  }

  /**
    * Loads Hadoop configurations
    * @return HadoopConfigs, otherwise throws a RuntimeException
    */
  def loadHadoopConf(): HadoopConfigs = {
    import pureconfig._
    import pureconfig.generic.auto._ // To be kept, even though intellij didn't recognize its usage

    implicit val offsetConvert: ConfigReader[LogLevel] =
      deriveEnumerationReader[LogLevel]

    ConfigSource.default
      .at("hadoop")
      .load[HadoopConfigs] match {
      case Right(conf) => conf
      case Left(thr) =>
        throw new RuntimeException(
          s"Error when trying to load Hadoop configuration : ${thr.toList.mkString("\n")}"
        )
    }
  }

  /**
    * Loads Cassandra configurations
    * @return CassandraConfigs, otherwise throws a RuntimeException
    */
  def loadCassandraConf(): CassandraConfigs = {
    import pureconfig._
    import pureconfig.generic.auto._ // To be kept, even though intellij didn't recognize its usage

    implicit val offsetConvert: ConfigReader[LogLevel] =
      deriveEnumerationReader[LogLevel]

    ConfigSource.default
      .at("cassandra")
      .load[CassandraConfigs] match {
      case Right(conf) => conf
      case Left(thr) =>
        throw new RuntimeException(
          s"Error when trying to load Cassandra configuration : ${thr.toList.mkString("\n")}"
        )
    }
  }
}
