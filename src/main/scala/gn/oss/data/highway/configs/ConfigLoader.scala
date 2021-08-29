package gn.oss.data.highway.configs

import gn.oss.data.highway.models.LogLevel
import pureconfig.{ConfigReader, ConfigSource}
import pureconfig.generic.semiauto._

case class ConfigLoader() {

  def loadConfigs[T: ConfigReader](param: String): T = {
    import pureconfig._
    val confAsString = ConfigSource.defaultApplication.value() match {
      case Right(conf) =>
        conf.toString.substring(19, conf.toString.length - 1)
      case Left(thr) =>
        throw new RuntimeException(
          s"Error when trying to load configuration : ${thr.toList.mkString("\n")}"
        )
    }
    loadConfigsFromString(param, confAsString)
  }

  def loadConfigsFromString[T: ConfigReader](
      param: String,
      confAsString: String
  ): T = {
    implicit val offsetConvert: ConfigReader[LogLevel] =
      deriveEnumerationReader[LogLevel]
    ConfigSource.string(confAsString).at(param).load[T] match {
      case Right(config) => config
      case Left(thr) =>
        throw new RuntimeException(
          s"Error when trying to load configuration : ${thr.toList.mkString("\n")}"
        )
    }
  }
}
