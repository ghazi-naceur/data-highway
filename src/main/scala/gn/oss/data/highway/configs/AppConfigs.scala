package gn.oss.data.highway.configs

import pureconfig.generic.auto._

case class AppConfigs(tmpWorkDir: String)
trait AppUtils {
  val appConf: AppConfigs = ConfigLoader().loadConfigs[AppConfigs]("app")
}
