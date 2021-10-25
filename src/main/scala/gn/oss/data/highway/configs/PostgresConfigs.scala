package gn.oss.data.highway.configs

import pureconfig.generic.auto._

case class PostgresConfigs(host: String, port: String, user: String, password: String)
trait PostgresUtils {
  val postgresConf: PostgresConfigs = ConfigLoader().loadConfigs[PostgresConfigs]("postgres")
}
