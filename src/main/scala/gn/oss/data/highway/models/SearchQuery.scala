package gn.oss.data.highway.models

sealed trait SearchQuery

case object MatchAllQuery extends SearchQuery

case class MatchQuery(field: Field) extends SearchQuery

case class MultiMatchQuery(values: List[String]) extends SearchQuery

case class TermQuery(field: Field) extends SearchQuery

case class TermsQuery(field: FieldValues) extends SearchQuery

case class CommonTermsQuery(field: Field) extends SearchQuery

case class QueryStringQuery(query: String) extends SearchQuery

case class SimpleStringQuery(query: String) extends SearchQuery

case class PrefixQuery(prefix: Prefix) extends SearchQuery

case class MoreLikeThisQuery(likeFields: LikeFields) extends SearchQuery

case class RangeQuery(rangeField: RangeField) extends SearchQuery

case class ExistsQuery(fieldName: String) extends SearchQuery

case class WildcardQuery(field: Field) extends SearchQuery

case class RegexQuery(field: Field) extends SearchQuery

case class FuzzyQuery(field: Field) extends SearchQuery

case class IdsQuery(ids: List[String]) extends SearchQuery

case class BoolMatchPhraseQuery(boolFilter: BoolFilter, fields: List[Field]) extends SearchQuery
