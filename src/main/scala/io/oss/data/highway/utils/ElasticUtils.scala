package io.oss.data.highway.utils

import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}

case class ElasticUtils(esNodes: String) {

  val client: ElasticClient = {
    val props = ElasticProperties(esNodes)
    ElasticClient(JavaClient(props))
  }
}
