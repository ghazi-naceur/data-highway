package io.oss.data.highway.models

sealed trait ElasticOperation

case class IndexCreation(indexName: String, mapping: Option[String])
    extends ElasticOperation

case class IndexDeletion(indexName: String) extends ElasticOperation
