package gn.oss.data.highway.configs

import pureconfig.generic.auto._

case class CassandraConfigs(host: String = "", port: String = "")
trait CassandraUtils {
  val cassandraConf: CassandraConfigs = ConfigLoader().loadConfigs[CassandraConfigs]("cassandra")
}
