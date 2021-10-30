package gn.oss.data.highway.engine.extractors

import com.sksamuel.elastic4s.requests.searches.{SearchHit, SearchResponse}
import com.sksamuel.elastic4s.{RequestFailure, RequestSuccess}
import gn.oss.data.highway.configs.{ElasticUtils, HdfsUtils}
import gn.oss.data.highway.engine.sinks._
import gn.oss.data.highway.models.DataHighwayErrorObj.DataHighwayFileError
import gn.oss.data.highway.utils.{FilesUtils, HdfsUtils, SharedUtils}
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.apache.log4j.Logger

import java.util.UUID
import scala.annotation.tailrec
import cats.implicits._
import gn.oss.data.highway.models.DataHighwayRuntimeException.{
  MustHaveExplicitSaveModeError,
  MustHaveExplicitFileSystemError,
  MustHaveSearchQueryError,
  MustNotHaveExplicitSaveModeError
}
import gn.oss.data.highway.models.{
  BoolFilter,
  BoolMatchPhraseQuery,
  Cassandra,
  CommonTermsQuery,
  Consistency,
  DataHighwayErrorResponse,
  DataHighwaySuccessResponse,
  Elasticsearch,
  ExistsQuery,
  Field,
  FieldValues,
  File,
  FuzzyQuery,
  GenericRangeField,
  HDFS,
  IdsQuery,
  JSON,
  Kafka,
  LikeFields,
  Local,
  MatchAllQuery,
  MatchQuery,
  MoreLikeThisQuery,
  MultiMatchQuery,
  Must,
  MustNot,
  Output,
  Postgres,
  Prefix,
  PrefixQuery,
  QueryStringQuery,
  RangeField,
  RangeQuery,
  RegexQuery,
  Should,
  SimpleStringQuery,
  Storage,
  TermQuery,
  TermsQuery,
  WildcardQuery
}

object ElasticExtractor extends ElasticUtils with HdfsUtils {

  val logger: Logger = Logger.getLogger(ElasticExtractor.getClass.getName)

  /**
    * Gets the first 10 documents from an Index
    *
    * @param in The Elasticsearch index
    * @return Json, otherwise an Error
    */
  def getTenRandomDocsFrom(in: String): Either[Exception, Json] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    esClient.execute { search(in).matchAllQuery() }.await match {
      case RequestSuccess(status, body, headers, result) =>
        logger.info(s"status: '$status', body: '$body', headers: '$headers', result: '$result'")
        Right(
          result.hits.hits.toList
            .map(hits => hits.sourceAsMap.mapValues(_.toString))
            .asJson
        )
      case RequestFailure(status, body, headers, error) =>
        logger.info(s"status: '$status', body: '$body', headers: '$headers', result: '$error'")
        Left(error.asException)
    }
  }

  /**
    * Searches for documents using Elasticsearch MatchAllQuery
    *
    * @param in The Elasticsearch index
    * @return List of SearchHit
    */
  private def searchWithMatchAllQuery(in: String): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    val result = esClient.execute { search(in).matchAllQuery() scroll "1m" }.await.result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch MatchQuery
    *
    * @param in The Elasticsearch index
    * @param field The filter field
    * @return List of SearchHit
    */
  private def searchWithMatchQuery(in: String, field: Field): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    val result = esClient.execute {
      search(in).matchQuery(field.name, field.value) scroll "1m"
    }.await.result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch MultiMatchQuery
    *
    * @param in The Elasticsearch index
    * @param values The list of values to search for
    * @return List of SearchHit
    */
  private def searchWithMultiMatchQuery(
      in: String,
      values: List[String]
  ): Either[Throwable, List[SearchHit]] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    Either.catchNonFatal {
      val request = values.map(value => search(in).query(value).size(10000))
      esClient.execute { multi(request) }.await.result.successes.toList
        .flatMap(searchResponse => searchResponse.hits.hits)
    }
  }

  /**
    * Searches for documents using Elasticsearch TermQuery
    *
    * @param in The Elasticsearch index
    * @param field The filter field
    * @return List of SearchHit
    */
  private def searchWithTermQuery(in: String, field: Field): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    val result = esClient.execute {
      search(in).query(termQuery(field.name, field.value)) scroll "1m"
    }.await.result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch TermsQuery
    *
    * @param in The Elasticsearch index
    * @param fieldValues The filter field name with multiple values
    * @return List of SearchHit
    */
  private def searchWithTermsQuery(in: String, fieldValues: FieldValues): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    val result = esClient.execute {
      search(in).query(termsQuery(fieldValues.name, fieldValues.values)) scroll "1m"
    }.await.result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch CommonTermsQuery
    *
    * @param in The Elasticsearch index
    * @param field The filter field
    * @return List of SearchHit
    */
  private def searchWithCommonTermsQuery(in: String, field: Field): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    val result = esClient.execute {
      search(in).query(commonTermsQuery(field.name, field.value)) scroll "1m"
    }.await.result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch QueryStringQuery
    *
    * @param in The Elasticsearch index
    * @param strQuery The elasticsearch string query
    * @return List of SearchHit
    */
  private def searchWithQueryStringQuery(in: String, strQuery: String): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    val result = esClient.execute {
      search(in).query(queryStringQuery(strQuery)) scroll "1m"
    }.await.result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch SimpleStringQuery
    *
    * @param in The Elasticsearch index
    * @param strQuery The elasticsearch string query
    * @return List of SearchHit
    */
  private def searchWithSimpleStringQuery(in: String, strQuery: String): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    val result = esClient.execute {
      search(in).query(simpleStringQuery(strQuery)) scroll "1m"
    }.await.result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch PrefixQuery
    *
    * @param in The Elasticsearch index
    * @param prefix The filter prefix
    * @return List of SearchHit
    */
  private def searchWithPrefixQuery(in: String, prefix: Prefix): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    val result = esClient.execute {
      search(in).query(prefixQuery(prefix.fieldName, prefix.value)) scroll "1m"
    }.await.result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch MoreLikeThisQuery
    *
    * @param in The Elasticsearch index
    * @param likeFields The filter fields
    * @return List of SearchHit
    */
  private def searchWithMoreLikeThisQuery(in: String, likeFields: LikeFields): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    import com.sksamuel.elastic4s.requests.searches.queries.MoreLikeThisQuery
    val result = esClient.execute {
      search(in).query(
        MoreLikeThisQuery(likeFields.fields, likeFields.likeTexts)
          .minTermFreq(1)
          .maxQueryTerms(12)
      ) scroll "1m"
    }.await.result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch RangeQuery
    *
    * @param in The Elasticsearch index
    * @param range The filter range field
    * @return List of SearchHit
    */
  private def searchWithRangeQuery(in: String, range: RangeField): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    import com.sksamuel.elastic4s.requests.searches.queries.RangeQuery
    val rangeField = GenericRangeField.computeTypedRangeField(range)
    val result = esClient.execute {
      search(in).query(
        RangeQuery(rangeField.name, lte = rangeField.lte, gte = rangeField.gte)
      ) scroll "1m"
    }.await.result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch ExistsQuery
    *
    * @param in The Elasticsearch index
    * @param fieldName The filter field name
    * @return List of SearchHit
    */
  private def searchWithExistsQuery(in: String, fieldName: String): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    val result = esClient.execute {
      search(in).query(existsQuery(fieldName)) scroll "1m"
    }.await.result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch ExistsQuery
    *
    * @param in The Elasticsearch index
    * @param field The filter field name
    * @return List of SearchHit
    */
  private def searchWithWildcardQuery(in: String, field: Field): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    val result = esClient.execute {
      search(in).query(wildcardQuery(field.name, field.value)) scroll "1m"
    }.await.result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch RegexQuery
    *
    * @param in The Elasticsearch index
    * @param field The filter field name
    * @return List of SearchHit
    */
  private def searchWithRegexQuery(in: String, field: Field): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    val result = esClient.execute {
      search(in).query(regexQuery(field.name, field.value)) scroll "1m"
    }.await.result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch FuzzyQuery
    *
    * @param in The Elasticsearch index
    * @param field The filter field name
    * @return List of SearchHit
    */
  private def searchWithFuzzyQuery(in: String, field: Field): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    val result = esClient.execute {
      search(in).query(fuzzyQuery(field.name, field.value)) scroll "1m"
    }.await.result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch IdsQuery
    *
    * @param in The Elasticsearch index
    * @param ids The filter Elasticsearch ids
    * @return List of SearchHit
    */
  private def searchWithIdsQuery(in: String, ids: List[String]): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    val result = esClient.execute {
      search(in).query(idsQuery(ids)) scroll "1m"
    }.await.result
    collectSearchHits(result)
  }

  /**
    * Searches for documents using Elasticsearch BoolMatchPhraseQuery
    *
    * @param in The Elasticsearch index
    * @param boolFilter The bool filter. It could have one of these values : Must, MustNot or Should
    * @param fields The filter fields
    * @return List of SearchHit
    */
  private def searchWithBoolMatchPhraseQuery(
      in: String,
      boolFilter: BoolFilter,
      fields: List[Field]
  ): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    val query = boolQuery()
    val queries = fields.map(field => {
      query.must(matchPhraseQuery(field.name, field.value))
    })
    val searchQuery = boolFilter match {
      case Must    => search(in).query(bool(queries, List(), List())) scroll "1m"
      case MustNot => search(in).query(bool(List(), List(), queries)) scroll "1m"
      case Should  => search(in).query(bool(List(), queries, List())) scroll "1m"
    }
    val result = esClient.execute { searchQuery }.await.result
    collectSearchHits(result)
  }

  /**
    * Saves documents found in Elasticsearch index
    *
    * @param input The Elasticsearch index
    * @param output The output base folder
    * @param storage The output file system storage
    * @param consistency The file saving mode
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  def saveDocuments(
      input: Elasticsearch,
      output: Output,
      storage: Option[Storage],
      consistency: Option[Consistency]
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    // todo maybe tempoEntity with timestamp
    val (temporaryPath, tempoBasePath) =
      SharedUtils.setTempoFilePath("elasticsearch-extractor", storage)
    val result = consistency match {
      case Some(_) =>
        handleRouteWithExplicitSaveMode(input, output, storage, consistency, temporaryPath)
      case None =>
        handleRouteWithImplicitSaveMode(input, output, temporaryPath)
    }
    cleanupTmp(tempoBasePath, storage)
    SharedUtils.constructIOResponse(input, output, result)
  }

  private def handleRouteWithImplicitSaveMode(
      input: Elasticsearch,
      output: Output,
      temporaryPath: String
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    output match {
      case elasticsearch @ Elasticsearch(_, _, _) =>
        searchDocsUsingSearchQuery(input, Some(Local), temporaryPath)
        ElasticSink
          .handleElasticsearchChannel(File(JSON, temporaryPath), elasticsearch, Some(Local))
      case kafka @ Kafka(_, _) =>
        searchDocsUsingSearchQuery(input, Some(Local), temporaryPath)
        KafkaSink.handleKafkaChannel(File(JSON, temporaryPath), kafka, Some(Local))
      case _ => Left(MustNotHaveExplicitSaveModeError)
    }
  }

  private def handleRouteWithExplicitSaveMode(
      input: Elasticsearch,
      output: Output,
      storage: Option[Storage],
      consistency: Option[Consistency],
      temporaryPath: String
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    output match {
      case file @ File(_, _) =>
        searchDocsUsingSearchQuery(input, storage, temporaryPath)
        BasicSink.handleChannel(File(JSON, temporaryPath), file, storage, consistency)
      case cassandra @ Cassandra(_, _) =>
        searchDocsUsingSearchQuery(input, Some(Local), temporaryPath)
        CassandraSink
          .handleCassandraChannel(File(JSON, temporaryPath), cassandra, Some(Local), consistency)
      case postgres @ Postgres(_, _) =>
        searchDocsUsingSearchQuery(input, Some(Local), temporaryPath)
        PostgresSink
          .handlePostgresChannel(File(JSON, temporaryPath), postgres, Some(Local), consistency)
      case _ => Left(MustHaveExplicitSaveModeError)
    }
  }

  private def searchDocsUsingSearchQuery(
      input: Elasticsearch,
      storage: Option[Storage],
      temporaryPath: String
  ): Either[Throwable, List[Unit]] = {
    storage match {
      case Some(filesystem) =>
        input.searchQuery match {
          case Some(query) =>
            query match {
              case MatchAllQuery =>
                searchWithMatchAllQuery(input.index)
                  .traverse(searchHit => saveSearchHit(searchHit, temporaryPath, filesystem))

              case MatchQuery(field) =>
                searchWithMatchQuery(input.index, field)
                  .traverse(searchHit => saveSearchHit(searchHit, temporaryPath, filesystem))

              case MultiMatchQuery(values) =>
                searchWithMultiMatchQuery(input.index, values).flatMap(hits => {
                  hits.traverse(searchHit => saveSearchHit(searchHit, temporaryPath, filesystem))
                })

              case TermQuery(field) =>
                searchWithTermQuery(input.index, field)
                  .traverse(searchHit => saveSearchHit(searchHit, temporaryPath, filesystem))

              case TermsQuery(fieldValues) =>
                searchWithTermsQuery(input.index, fieldValues)
                  .traverse(searchHit => saveSearchHit(searchHit, temporaryPath, filesystem))

              case CommonTermsQuery(field) =>
                searchWithCommonTermsQuery(input.index, field)
                  .traverse(searchHit => saveSearchHit(searchHit, temporaryPath, filesystem))

              case QueryStringQuery(query) =>
                searchWithQueryStringQuery(input.index, query)
                  .traverse(searchHit => saveSearchHit(searchHit, temporaryPath, filesystem))

              case SimpleStringQuery(query) =>
                searchWithSimpleStringQuery(input.index, query)
                  .traverse(searchHit => saveSearchHit(searchHit, temporaryPath, filesystem))

              case PrefixQuery(query) =>
                searchWithPrefixQuery(input.index, query)
                  .traverse(searchHit => saveSearchHit(searchHit, temporaryPath, filesystem))

              case MoreLikeThisQuery(likeFields) =>
                searchWithMoreLikeThisQuery(input.index, likeFields)
                  .traverse(searchHit => saveSearchHit(searchHit, temporaryPath, filesystem))

              case RangeQuery(rangeField) =>
                searchWithRangeQuery(input.index, rangeField)
                  .traverse(searchHit => saveSearchHit(searchHit, temporaryPath, filesystem))

              case ExistsQuery(fieldName) =>
                searchWithExistsQuery(input.index, fieldName)
                  .traverse(searchHit => saveSearchHit(searchHit, temporaryPath, filesystem))

              case WildcardQuery(field) =>
                searchWithWildcardQuery(input.index, field)
                  .traverse(searchHit => saveSearchHit(searchHit, temporaryPath, filesystem))

              case RegexQuery(field) =>
                searchWithRegexQuery(input.index, field)
                  .traverse(searchHit => saveSearchHit(searchHit, temporaryPath, filesystem))

              case FuzzyQuery(field) =>
                searchWithFuzzyQuery(input.index, field)
                  .traverse(searchHit => saveSearchHit(searchHit, temporaryPath, filesystem))

              case IdsQuery(ids) =>
                searchWithIdsQuery(input.index, ids)
                  .traverse(searchHit => saveSearchHit(searchHit, temporaryPath, filesystem))

              case BoolMatchPhraseQuery(boolFilter, fields) =>
                searchWithBoolMatchPhraseQuery(input.index, boolFilter, fields).traverse(
                  searchHit => saveSearchHit(searchHit, temporaryPath, filesystem)
                )
            }
          case None => Left(MustHaveSearchQueryError)
        }
      case None => Left(MustHaveExplicitFileSystemError)
    }
  }

  /**
    * Cleanups the folder
    *
    * @param output The tmp suffix path
    * @param storage The tmp file system storage
    * @return AnyVal, otherwise a Throwable
    */
  private def cleanupTmp(output: String, storage: Option[Storage]): Either[Throwable, AnyVal] = {
    storage match {
      case Some(filesystem) =>
        filesystem match {
          case Local => FilesUtils.delete(output)
          case HDFS  => HdfsUtils.delete(fs, output)
        }
      case None => Left(MustHaveExplicitFileSystemError)
    }
  }

  /**
    * Collects search hits using the previous search response
    *
    * @param result The previous search response
    * @return a List of SearchHit
    */
  private def collectSearchHits(result: SearchResponse): List[SearchHit] = {
    result.scrollId match {
      case Some(scrollId) => scrollOnDocs(scrollId, result.hits.hits.toList)
      case None           => List[SearchHit]()
    }
  }

  /**
    * Scrolls recursively on Search hits using the ScrollId to collect hits
    *
    * @param scrollId The Scroll Id
    * @param hits The ES records/documents
    * @return a List of SearchHit
    */
  @tailrec
  private def scrollOnDocs(scrollId: String, hits: List[SearchHit]): List[SearchHit] = {
    import com.sksamuel.elastic4s.ElasticDsl._
    val resp = esClient.execute {
      searchScroll(scrollId).keepAlive("1m")
    }.await.result
    if (resp.hits.hits.isEmpty) hits
    else {
      resp.scrollId match {
        case Some(scrollId) => scrollOnDocs(scrollId, hits ::: resp.hits.hits.toList)
        case None           => hits
      }
    }
  }

  /**
    * Saves an ES document as a Json file
    *
    * @param out The output File entity
    * @param storage The output file system : Local or HDFS
    * @return Unit, otherwise a Throwable
    */
  private def saveSearchHit(
      searchHit: SearchHit,
      out: String,
      storage: Storage
  ): Either[Throwable, Unit] = {
    storage match {
      // todo use SharedUtils.setTempoFilePath(), instead of generating a path here
      case HDFS =>
        HdfsUtils.save(
          fs,
          s"$out/${searchHit.index}/es-${searchHit.id}-${UUID.randomUUID()}-${System
            .currentTimeMillis()}.${JSON.extension}",
          searchHit.sourceAsMap.mapValues(_.toString).asJson.noSpaces
        )
      case Local =>
        FilesUtils.createFile(
          s"$out/${searchHit.index}",
          s"es-${searchHit.id}-${UUID.randomUUID()}-${System.currentTimeMillis()}.${JSON.extension}",
          searchHit.sourceAsMap.mapValues(_.toString).asJson.noSpaces
        )
    }
  }
}
