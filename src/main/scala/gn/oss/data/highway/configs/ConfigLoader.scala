package gn.oss.data.highway.configs

import gn.oss.data.highway.models.LogLevel
import pureconfig.{ConfigReader, ConfigSource}
import pureconfig.generic.semiauto._

case class ConfigLoader() {

  /**
    * Loads Config class from the default application.conf file
    *
    * @param param The configuration parameter name to be extracted
    * @tparam T The Config class that maps with @param parameter
    * @return The Config class
    */
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

  /**
    * Loads Config class from string configuration
    *
    * @param param The configuration parameter name to be extracted
    * @param confAsString The configuration as string
    * @tparam T The Config class that maps with @param parameter
    * @return The Config class
    */
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
