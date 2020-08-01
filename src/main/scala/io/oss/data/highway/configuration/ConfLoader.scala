package io.oss.data.highway.configuration

import cats.syntax.either._
import io.oss.data.highway.model.DataHighwayError.BulkErrorAccumulator
import io.oss.data.highway.model.Route

object ConfLoader {

  def loadConf(): Either[BulkErrorAccumulator, Route] = {
    import pureconfig._
    import pureconfig.generic.auto._ // To be kept, even though intellij didn't recognize its usage

    ConfigSource.default
      .at("route")
      .load[Route]
      .leftMap(thrs => BulkErrorAccumulator(thrs))
  }
}
