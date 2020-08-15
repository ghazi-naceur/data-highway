package io.oss.data.highway.configuration

import cats.syntax.either._
import io.oss.data.highway.model.DataHighwayError.BulkErrorAccumulator
import io.oss.data.highway.model.{KafkaMode, Offset, Route}
import pureconfig.generic.semiauto._

object ConfLoader {

  def loadConf(): Either[BulkErrorAccumulator, Route] = {
    import pureconfig._
    import pureconfig.generic.auto._ // To be kept, even though intellij didn't recognize its usage

    implicit val offsetConvert: ConfigReader[Offset] =
      deriveEnumerationReader[Offset]

    implicit val kafkaModeConvert: ConfigReader[KafkaMode] =
      deriveEnumerationReader[KafkaMode]

    ConfigSource.default
      .at("route")
      .load[Route]
      .leftMap(thrs => BulkErrorAccumulator(thrs))
  }
}
