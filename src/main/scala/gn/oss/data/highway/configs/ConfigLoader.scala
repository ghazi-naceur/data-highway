package gn.oss.data.highway.configs

import com.typesafe.config.Config
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
    val configObject = ConfigSource.defaultApplication.value() match {
      case Right(conf) => conf.toConfig
      case Left(thr) =>
        throw new RuntimeException(
          s"Error when trying to load configuration : ${thr.toList.mkString("\n")}"
        )
    }
    loadConfigsFromConfigObject(param, configObject)
  }

  /**
    * Loads Config class from Config object
    *
    * @param param The configuration parameter name to be extracted
    * @param conf The input configuration
    * @tparam T The Config class that maps with @param parameter
    * @return The Config class
    */
  def loadConfigsFromConfigObject[T: ConfigReader](
      param: String,
      conf: Config
  ): T = {
    implicit val offsetConvert: ConfigReader[LogLevel] =
      deriveEnumerationReader[LogLevel]
    ConfigSource.fromConfig(conf).at(param).load[T] match {
      case Right(config) => config
      case Left(errors) =>
        throw new RuntimeException(
          s"Error when trying to load configuration : ${errors.toList.mkString("\n")}"
        )
    }
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
