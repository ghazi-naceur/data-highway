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
import com.sksamuel.elastic4s.requests.searches.{SearchHit, SearchResponse}
import io.oss.data.highway.models.{
  Field,
  JSON,
  MatchAllQuery,
  MatchQuery,
  MultiMatchQuery,
  SearchQuery
}

import java.util.UUID

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
    * Searches for documents using Elasticsearch MatchAllQuery
    * @param in The Elasticsearch index
    * @return List of SearchHit
    */
  def searchWithMatchAllQuery(in: String): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._

    val matchAllRes = esClient
      .execute {
        search(in).matchAllQuery() scroll "1m"
      }
      .await
      .result

    collectSearchHits(matchAllRes)
  }

  /**
    * Searches for documents using Elasticsearch MatchQuery
    * @param in The Elasticsearch index
    * @param field The filter field
    * @return List of SearchHit
    */
  def searchWithMatchQuery(in: String, field: Field): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._

    val matchAllRes = esClient
      .execute {
        search(in).matchQuery(field.name, field.value) scroll "1m"
      }
      .await
      .result

    collectSearchHits(matchAllRes)
  }

  /**
    * Searches for documents using Elasticsearch MultiMatchQuery
    * @param in The Elasticsearch index
    * @param values The list of values to search for
    * @return List of SearchHit
    */
  def searchWithMultiMatchQuery(
      in: String,
      values: List[String]): Either[Throwable, List[SearchHit]] = {
    import com.sksamuel.elastic4s.ElasticDsl._

    val request = values.map(value => search(in).query(value).size(10000))

    Either.catchNonFatal {
      esClient
        .execute {
          multi(
            request
          )
        }
        .await
        .result
        .successes
        .toList
        .flatMap(searchResponse => {
          searchResponse.hits.hits
        })
    }
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
        searchWithMatchAllQuery(in).traverse(saveSearchHit(out))

      case MatchQuery(field) =>
        searchWithMatchQuery(in, field).traverse(saveSearchHit(out))

      case MultiMatchQuery(values) =>
        searchWithMultiMatchQuery(in, values).flatMap(hits => {
          hits.traverse(saveSearchHit(out))
        })

      case _ => Either.catchNonFatal(List())

    }
  }

  private def collectSearchHits(
      matchAllRes: SearchResponse): List[SearchHit] = {
    matchAllRes.scrollId match {
      case Some(scrollId) =>
        scrollOnDocs(scrollId, matchAllRes.hits.hits.toList)
      case None =>
        List[SearchHit]()
    }
  }

  @tailrec
  def scrollOnDocs(scrollId: String, hits: List[SearchHit]): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._
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

  private def saveSearchHit(
      out: String): SearchHit => Either[Throwable, Unit] = {
    (searchHit: SearchHit) =>
      {
        FilesUtils.save(
          s"$out/${searchHit.index}",
          s"es-${searchHit.id}-${UUID.randomUUID()}-${System
            .currentTimeMillis()}.${JSON.extension}",
          searchHit.sourceAsMap.mapValues(_.toString).asJson.noSpaces
        )
      }
  }
}
