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
  BoolFilter,
  BoolMatchPhraseQuery,
  CommonTermsQuery,
  ExistsQuery,
  Field,
  FieldValues,
  FuzzyQuery,
  GenericRangeField,
  IdsQuery,
  JSON,
  LikeFields,
  MatchAllQuery,
  MatchQuery,
  MoreLikeThisQuery,
  MultiMatchQuery,
  Must,
  MustNot,
  Prefix,
  PrefixQuery,
  QueryStringQuery,
  RangeField,
  RangeQuery,
  RegexQuery,
  SearchQuery,
  Should,
  SimpleStringQuery,
  TermQuery,
  TermsQuery,
  WildcardQuery
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

    val result = esClient
      .execute {
        search(in).matchAllQuery() scroll "1m"
      }
      .await
      .result

    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch MatchQuery
    * @param in The Elasticsearch index
    * @param field The filter field
    * @return List of SearchHit
    */
  def searchWithMatchQuery(in: String, field: Field): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._

    val result = esClient
      .execute {
        search(in).matchQuery(field.name, field.value) scroll "1m"
      }
      .await
      .result

    collectSearchHits(result)
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
    * Searches for documents using Elasticsearch TermQuery
    * @param in The Elasticsearch index
    * @param field The filter field
    * @return List of SearchHit
    */
  def searchWithTermQuery(in: String, field: Field): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._

    val result =
      esClient
        .execute {
          search(in).query(termQuery(field.name, field.value)) scroll "1m"
        }
        .await
        .result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch TermsQuery
    * @param in The Elasticsearch index
    * @param fieldValues The filter field name with multiple values
    * @return List of SearchHit
    */
  def searchWithTermsQuery(in: String,
                           fieldValues: FieldValues): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._

    val result =
      esClient
        .execute {
          search(in).query(termsQuery(fieldValues.name, fieldValues.values)) scroll "1m"
        }
        .await
        .result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch CommonTermsQuery
    * @param in The Elasticsearch index
    * @param field The filter field
    * @return List of SearchHit
    */
  def searchWithCommonTermsQuery(in: String, field: Field): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._

    val result =
      esClient
        .execute {
          search(in).query(commonTermsQuery(field.name, field.value)) scroll "1m"
        }
        .await
        .result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch QueryStringQuery
    * @param in The Elasticsearch index
    * @param strQuery The elasticsearch string query
    * @return List of SearchHit
    */
  def searchWithQueryStringQuery(in: String,
                                 strQuery: String): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._

    val result =
      esClient
        .execute {
          search(in).query(queryStringQuery(strQuery)) scroll "1m"
        }
        .await
        .result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch SimpleStringQuery
    * @param in The Elasticsearch index
    * @param strQuery The elasticsearch string query
    * @return List of SearchHit
    */
  def searchWithSimpleStringQuery(in: String,
                                  strQuery: String): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._

    val result =
      esClient
        .execute {
          search(in).query(simpleStringQuery(strQuery)) scroll "1m"
        }
        .await
        .result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch PrefixQuery
    * @param in The Elasticsearch index
    * @param prefix The filter prefix
    * @return List of SearchHit
    */
  def searchWithPrefixQuery(in: String, prefix: Prefix): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._

    val result =
      esClient
        .execute {
          search(in).query(prefixQuery(prefix.fieldName, prefix.value)) scroll "1m"
        }
        .await
        .result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch MoreLikeThisQuery
    * @param in The Elasticsearch index
    * @param likeFields The filter fields
    * @return List of SearchHit
    */
  def searchWithMoreLikeThisQuery(in: String,
                                  likeFields: LikeFields): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    import com.sksamuel.elastic4s.requests.searches.queries.MoreLikeThisQuery

    val result =
      esClient
        .execute {
          search(in).query(
            MoreLikeThisQuery(likeFields.fields, likeFields.likeTexts)
              .minTermFreq(1)
              .maxQueryTerms(12)) scroll "1m"
        }
        .await
        .result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch RangeQuery
    * @param in The Elasticsearch index
    * @param range The filter range field
    * @return List of SearchHit
    */
  def searchWithRangeQuery(in: String, range: RangeField): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    import com.sksamuel.elastic4s.requests.searches.queries.RangeQuery
    val rangeField = GenericRangeField.computeTypedRangeField(range)
    val result =
      esClient
        .execute {
          search(in).query(
            RangeQuery(rangeField.name,
                       lte = rangeField.lte,
                       gte = rangeField.gte)) scroll "1m"
        }
        .await
        .result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch ExistsQuery
    * @param in The Elasticsearch index
    * @param fieldName The filter field name
    * @return List of SearchHit
    */
  def searchWithExistsQuery(in: String, fieldName: String): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._

    val result =
      esClient
        .execute {
          search(in).query(existsQuery(fieldName)) scroll "1m"
        }
        .await
        .result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch ExistsQuery
    * @param in The Elasticsearch index
    * @param field The filter field name
    * @return List of SearchHit
    */
  def searchWithWildcardQuery(in: String, field: Field): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._

    val result =
      esClient
        .execute {
          search(in).query(wildcardQuery(field.name, field.value)) scroll "1m"
        }
        .await
        .result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch RegexQuery
    * @param in The Elasticsearch index
    * @param field The filter field name
    * @return List of SearchHit
    */
  def searchWithRegexQuery(in: String, field: Field): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._

    val result =
      esClient
        .execute {
          search(in).query(regexQuery(field.name, field.value)) scroll "1m"
        }
        .await
        .result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch FuzzyQuery
    * @param in The Elasticsearch index
    * @param field The filter field name
    * @return List of SearchHit
    */
  def searchWithFuzzyQuery(in: String, field: Field): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._

    val result =
      esClient
        .execute {
          search(in).query(fuzzyQuery(field.name, field.value)) scroll "1m"
        }
        .await
        .result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch IdsQuery
    * @param in The Elasticsearch index
    * @param ids The filter Elasticsearch ids
    * @return List of SearchHit
    */
  def searchWithIdsQuery(in: String, ids: List[String]): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._

    val result =
      esClient
        .execute {
          search(in).query(idsQuery(ids)) scroll "1m"
        }
        .await
        .result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch BoolMatchPhraseQuery
    * @param in The Elasticsearch index
    * @param boolFilter The bool filter. It could have one of these values : Must, MustNot or Should
    * @param fields The filter fields
    * @return List of SearchHit
    */
  def searchWithBoolMatchPhraseQuery(in: String,
                                     boolFilter: BoolFilter,
                                     fields: List[Field]): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._

    val query = boolQuery()
    val queries = fields.map(field => {
      query.must(matchPhraseQuery(field.name, field.value))
    })

    val searchQuery = boolFilter match {
      case Must =>
        search(in).query(bool(queries, List(), List())) scroll "1m"
      case MustNot =>
        search(in).query(bool(List(), List(), queries)) scroll "1m"
      case Should =>
        search(in).query(bool(List(), queries, List())) scroll "1m"
    }

    val result =
      esClient
        .execute {
          searchQuery
        }
        .await
        .result
    collectSearchHits(result)
  }

  /**
    * Saves documents found in Elasticsearch index
    *
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

      case TermQuery(field) =>
        searchWithTermQuery(in, field).traverse(saveSearchHit(out))

      case TermsQuery(fieldValues) =>
        searchWithTermsQuery(in, fieldValues).traverse(saveSearchHit(out))

      case CommonTermsQuery(field) =>
        searchWithCommonTermsQuery(in, field).traverse(saveSearchHit(out))

      case QueryStringQuery(query) =>
        searchWithQueryStringQuery(in, query).traverse(saveSearchHit(out))

      case SimpleStringQuery(query) =>
        searchWithSimpleStringQuery(in, query).traverse(saveSearchHit(out))

      case PrefixQuery(query) =>
        searchWithPrefixQuery(in, query).traverse(saveSearchHit(out))

      case MoreLikeThisQuery(likeFields) =>
        searchWithMoreLikeThisQuery(in, likeFields).traverse(saveSearchHit(out))

      case RangeQuery(rangeField) =>
        searchWithRangeQuery(in, rangeField).traverse(saveSearchHit(out))

      case ExistsQuery(fieldName) =>
        searchWithExistsQuery(in, fieldName).traverse(saveSearchHit(out))

      case WildcardQuery(field) =>
        searchWithWildcardQuery(in, field).traverse(saveSearchHit(out))

      case RegexQuery(field) =>
        searchWithRegexQuery(in, field).traverse(saveSearchHit(out))

      case FuzzyQuery(field) =>
        searchWithFuzzyQuery(in, field).traverse(saveSearchHit(out))

      case IdsQuery(ids) =>
        searchWithIdsQuery(in, ids).traverse(saveSearchHit(out))

      case BoolMatchPhraseQuery(boolFilter, fields) =>
        searchWithBoolMatchPhraseQuery(in, boolFilter, fields).traverse(
          saveSearchHit(out))

      case _ => Either.catchNonFatal(List())

    }
  }

  private def collectSearchHits(result: SearchResponse): List[SearchHit] = {
    result.scrollId match {
      case Some(scrollId) =>
        scrollOnDocs(scrollId, result.hits.hits.toList)
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
