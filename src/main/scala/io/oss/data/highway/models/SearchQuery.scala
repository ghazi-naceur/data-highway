package io.oss.data.highway.models

case class Field(name: String, value: String)

sealed trait SearchQuery

case object MatchAllQuery extends SearchQuery

case class MatchQuery(field: Field) extends SearchQuery

case class MultiMatchQuery(values: List[String]) extends SearchQuery

case class TermQuery(field: Field) extends SearchQuery
