package gn.oss.data.highway.engine.ops

import gn.oss.data.highway.configs.ElasticUtils
import cats.implicits._
import gn.oss.data.highway.models.{
  DHErrorResponse,
  DataHighwayElasticResponse,
  DataHighwayErrorResponse,
  DataHighwaySuccessResponse,
  ElasticOperation,
  IndexCreation,
  IndexDeletion,
  IndexMapping
}

object ElasticAdminOps extends ElasticUtils {

  /**
    * Executes an Elasticsearch operation
    *
    * @param operation THe ES operation
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  def execute(
      operation: ElasticOperation
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
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
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  def createIndice(
      indexName: String
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    Either.catchNonFatal {
      val result = esClient.execute {
        createIndex(indexName)
      }.await.result
      if (result.acknowledged)
        DataHighwayElasticResponse(indexName, "Index created successfully")
      else
        DataHighwayElasticResponse(indexName, "Index is not created")
    }.leftMap(thr => DHErrorResponse(thr.getMessage, thr.getCause.toString, ""))
  }

  /**
    * Create an ES index with a mapping
    *
    * @param indexName The index to be created
    * @param mappings THe index mapping to be applied
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  def createIndice(
      indexName: String,
      mappings: String
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    Either.catchNonFatal {
      esClient.execute {
        createIndex(indexName)
      }.await.result
      val result = esClient.execute {
        putMapping(indexName).rawSource(mappings)
      }.await.result
      if (result.acknowledged)
        DataHighwayElasticResponse(indexName, "Index created successfully")
      else
        DataHighwayElasticResponse(indexName, "Index is not created")
    }.leftMap(thr => DHErrorResponse(thr.getMessage, thr.getCause.toString, ""))
  }

  /**
    * Deletes an ES index
    *
    * @param indexName The ES index to be deleted
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  def deleteIndice(
      indexName: String
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    Either.catchNonFatal {
      val result = esClient.execute {
        deleteIndex(indexName)
      }.await.result
      if (result.acknowledged)
        DataHighwayElasticResponse(indexName, "Index deleted successfully")
      else
        DataHighwayElasticResponse(indexName, "Index is not deleted")
    }.leftMap(thr => DHErrorResponse(thr.getMessage, thr.getCause.toString, ""))
  }

  /**
    * Adds a mapping to an ES index
    *
    * @param indexName The provided ES index
    * @param mappings The mapping to be applied
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  def addMapping(
      indexName: String,
      mappings: String
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    Either.catchNonFatal {
      val result = esClient.execute {
        putMapping(indexName).rawSource(mappings)
      }.await.result
      if (result.acknowledged)
        DataHighwayElasticResponse(indexName, "Mapping added successfully")
      else
        DataHighwayElasticResponse(indexName, "Mapping is not added")
    }.leftMap(thr => DHErrorResponse(thr.getMessage, thr.getCause.toString, ""))
  }
}
