package io.oss.data.highway.sinks

import com.sksamuel.elastic4s.requests.indexes.{CreateIndexResponse, PutMappingResponse}
import com.sksamuel.elastic4s.requests.indexes.admin.DeleteIndexResponse
import io.oss.data.highway.models.{ElasticOperation, IndexCreation, IndexDeletion, IndexMapping}
import cats.syntax.either._
import io.oss.data.highway.utils.ElasticUtils

object ElasticAdminOps extends ElasticUtils {

  def execute(operation: ElasticOperation): Either[Throwable, Product] = {
    operation match {
      case IndexCreation(indexName, optMapping) =>
        optMapping match {
          case Some(raw) =>
            createIndice(indexName, raw)
          case None =>
            createIndice(indexName)
        }
      case IndexDeletion(indexName) =>
        deleteIndice(indexName)
      case IndexMapping(indexName, mapping) =>
        addMapping(indexName, mapping)
    }
  }

  def createIndice(indexName: String): Either[Throwable, CreateIndexResponse] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    Either.catchNonFatal {
      esClient.execute {
        createIndex(indexName)
      }.await.result
    }
  }

  def createIndice(indexName: String, mappings: String): Either[Throwable, PutMappingResponse] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    Either.catchNonFatal {
      esClient.execute {
        createIndex(indexName)
      }.await.result
      esClient.execute {
        putMapping(indexName).rawSource(mappings)
      }.await.result
    }
  }

  def deleteIndice(indexName: String): Either[Throwable, DeleteIndexResponse] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    Either.catchNonFatal {
      esClient.execute {
        deleteIndex(indexName)
      }.await.result
    }
  }

  def addMapping(indexName: String, mappings: String): Either[Throwable, PutMappingResponse] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    Either.catchNonFatal {
      esClient.execute {
        putMapping(indexName).rawSource(mappings)
      }.await.result
    }
  }
}
