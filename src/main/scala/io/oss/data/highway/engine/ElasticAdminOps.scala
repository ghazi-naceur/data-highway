package io.oss.data.highway.engine

import com.sksamuel.elastic4s.requests.indexes.{CreateIndexResponse, PutMappingResponse}
import com.sksamuel.elastic4s.requests.indexes.admin.DeleteIndexResponse
import io.oss.data.highway.models.{ElasticOperation, IndexCreation, IndexDeletion, IndexMapping}
import cats.syntax.either._
import io.oss.data.highway.utils.ElasticUtils

object ElasticAdminOps extends ElasticUtils {

  /**
    * Executes an Elasticsearch operation
    *
    * @param operation THe ES operation
    * @return Product, other an Throwable
    */
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

  /**
    * Creates an ES index
    *
    * @param indexName THe index to be created
    * @return CreateIndexResponse, otherwise a Throwable
    */
  def createIndice(indexName: String): Either[Throwable, CreateIndexResponse] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    Either.catchNonFatal {
      esClient.execute {
        createIndex(indexName)
      }.await.result
    }
  }

  /**
    * Create an ES index with a mapping
    *
    * @param indexName The index to be created
    * @param mappings THe index mapping to be applied
    * @return PutMappingResponse, otherwise a Throwable
    */
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

  /**
    * Deletes an ES index
    *
    * @param indexName The ES index to be deleted
    * @return DeleteIndexResponse, otherwise a Throwable
    */
  def deleteIndice(indexName: String): Either[Throwable, DeleteIndexResponse] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    Either.catchNonFatal {
      esClient.execute {
        deleteIndex(indexName)
      }.await.result
    }
  }

  /**
    * Adds a mapping to an ES index
    *
    * @param indexName The provided ES index
    * @param mappings The mapping to be applied
    * @return PutMappingResponse, otherwise a Throwable
    */
  def addMapping(indexName: String, mappings: String): Either[Throwable, PutMappingResponse] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    Either.catchNonFatal {
      esClient.execute {
        putMapping(indexName).rawSource(mappings)
      }.await.result
    }
  }
}
