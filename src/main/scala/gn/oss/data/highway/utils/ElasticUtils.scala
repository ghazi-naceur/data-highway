package gn.oss.data.highway.utils

import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import gn.oss.data.highway.configs.{ConfigLoader, ElasticConfigs}
import pureconfig.generic.auto._

trait ElasticUtils {

  val esConf: ElasticConfigs = ConfigLoader().loadConfigs[ElasticConfigs]("elasticsearch")
  val esClient: ElasticClient = {
    val props = ElasticProperties(s"${esConf.host}:${esConf.port}")
    ElasticClient(JavaClient(props))
  }
}
