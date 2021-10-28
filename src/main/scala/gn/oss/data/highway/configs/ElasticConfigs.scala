package gn.oss.data.highway.configs

import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import pureconfig.generic.auto._

case class ElasticConfigs(host: String, port: String)

trait ElasticUtils {
  val esConf: ElasticConfigs = ConfigLoader().loadConfigs[ElasticConfigs]("elasticsearch")
  val esClient: ElasticClient = {
    val props = ElasticProperties(s"${esConf.host}:${esConf.port}")
    ElasticClient(JavaClient(props))
  }
}
