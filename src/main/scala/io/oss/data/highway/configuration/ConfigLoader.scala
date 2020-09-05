package io.oss.data.highway.configuration

import cats.syntax.either._
import io.oss.data.highway.model.DataHighwayError.BulkErrorAccumulator
import io.oss.data.highway.model.{LogLevel, Offset, Route}
import pureconfig.generic.semiauto._

case class ConfigLoader() {

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

  def loadSparkConf(): Either[BulkErrorAccumulator, SparkConfig] = {
    import pureconfig._
    import pureconfig.generic.auto._ // To be kept, even though intellij didn't recognize its usage

    implicit val offsetConvert: ConfigReader[LogLevel] =
      deriveEnumerationReader[LogLevel]

    ConfigSource.default
      .at("spark")
      .load[SparkConfig]
      .leftMap(thrs => BulkErrorAccumulator(thrs))
  }
}
