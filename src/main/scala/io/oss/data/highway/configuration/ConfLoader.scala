package io.oss.data.highway.configuration

import pureconfig.ConfigReader.Result
import pureconfig.ConfigSource


object ConfLoader {

  def loadConf(): Result[Conf] = {
    import pureconfig._
    import pureconfig.generic.auto._
    ConfigSource.default.load[Conf]
  }
}
