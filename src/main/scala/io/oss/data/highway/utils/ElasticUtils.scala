package io.oss.data.highway.utils

import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import io.oss.data.highway.configuration.{ConfigLoader, ElasticConfigs}

trait ElasticUtils {

  val esConf: ElasticConfigs = ConfigLoader().loadElasticConf()
  val esClient: ElasticClient = {
    val props = ElasticProperties(esConf.esNodes)
    ElasticClient(JavaClient(props))
  }
}
