package gn.oss.data.highway.engine.ops

import gn.oss.data.highway.configs.ElasticUtils
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import gn.oss.data.highway.models.{
  DataHighwayElasticResponse,
  DataHighwayError,
  DataHighwayErrorResponse,
  DataHighwaySuccessResponse,
  ElasticOperation,
  IndexCreation,
  IndexDeletion,
  IndexMapping
}
import gn.oss.data.highway.utils.Constants

object ElasticAdminOps extends ElasticUtils with LazyLogging {

  /**
    * Executes an Elasticsearch operation
    *
    * @param operation THe ES operation
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  def execute(operation: ElasticOperation): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    operation match {
      case IndexCreation(indexName, optMapping) =>
        optMapping match {
          case Some(raw) => createIndice(indexName, raw)
          case None      => createIndice(indexName)
        }
      case IndexDeletion(indexName)         => deleteIndice(indexName)
      case IndexMapping(indexName, mapping) => addMapping(indexName, mapping)
    }
  }

  /**
    * Creates an ES index
    *
    * @param indexName THe index to be created
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  def createIndice(indexName: String): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    Either.catchNonFatal {
      val result = esClient.execute {
        createIndex(indexName)
      }.await.result
      if (result.acknowledged) {
        logger.info(s"'$indexName' index is created successfully")
        DataHighwayElasticResponse(indexName, "Index created successfully")
      } else {
        logger.info(s"'$indexName' index is not created")
        DataHighwayElasticResponse(indexName, "Index is not created")
      }
    }.leftMap(thr => {
      logger.error(s"An error occurred when trying to create the index: ${DataHighwayError.prettyError(thr)}")
      DataHighwayError(thr.getMessage, Constants.EMPTY)
    })
  }

  /**
    * Creates an ES index with a mapping
    *
    * @param indexName The index to be created
    * @param mappings THe index mapping to be applied
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  def createIndice(indexName: String, mappings: String): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    Either.catchNonFatal {
      esClient.execute {
        createIndex(indexName)
      }.await.result
      val result = esClient.execute {
        putMapping(indexName).rawSource(mappings)
      }.await.result
      if (result.acknowledged) {
        logger.info(s"'$indexName' index is created successfully")
        DataHighwayElasticResponse(indexName, "Index created successfully")
      } else {
        logger.info(s"'$indexName' index is not created")
        DataHighwayElasticResponse(indexName, "Index is not created")
      }
    }.leftMap(thr => {
      logger.error(s"An error occurred when trying to create the index: ${DataHighwayError.prettyError(thr)}")
      DataHighwayError(thr.getMessage, Constants.EMPTY)
    })
  }

  /**
    * Deletes an ES index
    *
    * @param indexName The ES index to be deleted
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  def deleteIndice(indexName: String): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    Either.catchNonFatal {
      val result = esClient.execute {
        deleteIndex(indexName)
      }.await.result
      if (result.acknowledged) {
        logger.info(s"'$indexName' index is deleted successfully")
        DataHighwayElasticResponse(indexName, "Index deleted successfully")
      } else {
        logger.info(s"'$indexName' index is not deleted")
        DataHighwayElasticResponse(indexName, "Index is not deleted")
      }
    }.leftMap(thr => {
      logger.error(s"An error occurred when trying to delete the index: ${DataHighwayError.prettyError(thr)}")
      DataHighwayError(thr.getMessage, Constants.EMPTY)
    })
  }

  /**
    * Adds a mapping to an ES index
    *
    * @param indexName The provided ES index
    * @param mappings The mapping to be applied
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  def addMapping(indexName: String, mappings: String): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    Either.catchNonFatal {
      val result = esClient.execute {
        putMapping(indexName).rawSource(mappings)
      }.await.result
      if (result.acknowledged) {
        logger.info(s"'$indexName' index mapping added successfully")
        DataHighwayElasticResponse(indexName, "Mapping added successfully")
      } else {
        logger.info(s"'$indexName' index mapping is not added")
        DataHighwayElasticResponse(indexName, "Mapping is not added")
      }
    }.leftMap(thr => {
      logger.error(s"An error occurred when trying to add the mapping: ${DataHighwayError.prettyError(thr)}")
      DataHighwayError(thr.getMessage, Constants.EMPTY)
    })
  }
}
