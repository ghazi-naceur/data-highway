package io.oss.data.highway.configuration

import cats.syntax.either._
import io.oss.data.highway.model.DataHighwayError.BulkErrorAccumulator
import io.oss.data.highway.model.{LogLevel, Offset, Route}
import pureconfig.generic.semiauto._

case class ConfigLoader() {

  /**
    * Loads Data Highway Route configurations
    * @return a Route, otherwise a BulkErrorAccumulator
    */
  def loadConf(): Either[BulkErrorAccumulator, Route] = {
    import pureconfig._
    import pureconfig.generic.auto._ // To be kept, even though intellij didn't recognize its usage

    implicit val offsetConvert: ConfigReader[Offset] =
      deriveEnumerationReader[Offset]

    ConfigSource.default
      .at("route")
      .load[Route]
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
          s"Error when trying to load Spark configuration : ${thr.toList.mkString("\n")}")
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
          s"Error when trying to load Elasticsearch configuration : ${thr.toList.mkString("\n")}")
    }
  }
}
