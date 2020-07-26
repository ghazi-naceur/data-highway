package io.oss.data.highway.configuration

import cats.data.EitherNel
import cats.syntax.either._
import pureconfig.error.ConfigReaderFailures

object ConfLoader {

  def loadConf(): EitherNel[ConfigReaderFailures, Conf] = {
    import pureconfig._
    import pureconfig.generic.auto._ // To be kept, even though intellij didn't recognize its usage
    ConfigSource.default.load[Conf].toEitherNel
  }
}
