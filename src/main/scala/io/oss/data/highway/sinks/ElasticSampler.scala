package io.oss.data.highway.sinks

import cats.implicits.toTraverseOps
import com.sksamuel.elastic4s.{RequestFailure, RequestSuccess}
import io.circe.Json
import io.circe.syntax.EncoderOps
import io.oss.data.highway.utils.{ElasticUtils, FilesUtils}
import org.apache.log4j.Logger

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import cats.syntax.either._
import com.sksamuel.elastic4s.requests.searches.SearchHit
import io.oss.data.highway.models.{
  Field,
  MatchAllQuery,
  MatchQuery,
  SearchQuery
}

object ElasticSampler extends ElasticUtils {

  val logger: Logger = Logger.getLogger(ElasticSampler.getClass.getName)

  /**
    * Gets the first 10 documents from an Index
    * @param in The Elasticsearch index
    * @return Json, otherwise an Error
    */
  def getTenRandomDocsFrom(in: String): Either[Exception, Json] = {
    import com.sksamuel.elastic4s.ElasticDsl._

    esClient.execute {
      search(in).matchAllQuery()
    }.await match {
      case RequestSuccess(status, body, headers, result) =>
        logger.info(
          s"status: '$status', body: '$body', headers: '$headers', result: '$result'")
        Right(
          result.hits.hits.toList
            .map(hits => hits.sourceAsMap.mapValues(_.toString))
            .asJson)
      case RequestFailure(status, body, headers, error) =>
        logger.info(
          s"status: '$status', body: '$body', headers: '$headers', result: '$error'")
        Left(error.asException)
    }
  }

  /**
    * Scans and scrolls on an Index to get all documents
    * @param in The Elasticsearch index
    * @return List of SearchHit
    */
  def scanAndScroll(in: String): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._

    @tailrec
    def scrollOnDocs(scrollId: String,
                     hits: List[SearchHit]): List[SearchHit] = {
      val resp = esClient
        .execute {
          searchScroll(scrollId).keepAlive("1m")
        }
        .await
        .result
      if (resp.hits.hits.isEmpty) hits
      else {
        resp.scrollId match {
          case Some(scrollId) =>
            scrollOnDocs(scrollId, hits ::: resp.hits.hits.toList)
          case None =>
            hits
        }
      }
    }

    val matchAllRes = esClient
      .execute {
        search(in).matchAllQuery() scroll "1m"
      }
      .await
      .result

    matchAllRes.scrollId match {
      case Some(scrollId) =>
        scrollOnDocs(scrollId, matchAllRes.hits.hits.toList)
      case None =>
        List[SearchHit]()
    }
  }

  /**
    * Searches for documents using Elasticsearch MatchQuery
    * @param in The Elasticsearch index
    * @param field The filter field
    * @return List of SearchHit
    */
  def searchWithMatchQuery(in: String, field: Field): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._

    esClient
      .execute {
        search(in).matchQuery(field.name, field.value)
      }
      .await
      .result
      .hits
      .hits
      .toList
  }

  /**
    * Saves documents found in Elasticsearch index
    * @param in The Elasticsearch index
    * @param out The output base folder
    * @param searchQuery The Elasticsearch query
    * @return List of Unit, otherwise an Error
    */
  def saveDocuments(in: String,
                    out: String,
                    searchQuery: SearchQuery): Either[Throwable, List[Unit]] = {
    searchQuery match {
      case MatchAllQuery =>
        scanAndScroll(in).traverse(searchHit => {
          FilesUtils.save(
            s"$out/${searchHit.index}",
            s"es-${searchHit.id}.json",
            searchHit.sourceAsMap.mapValues(_.toString).asJson.noSpaces)
        })

      case MatchQuery(field) =>
        searchWithMatchQuery(in, field).traverse(searchHit => {
          FilesUtils.save(
            s"$out/${searchHit.index}",
            s"es-${searchHit.id}.json",
            searchHit.sourceAsMap.mapValues(_.toString).asJson.noSpaces)
        })

      case _ => Either.catchNonFatal(List())

    }
  }
}
