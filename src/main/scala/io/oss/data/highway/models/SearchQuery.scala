package io.oss.data.highway.models

case class Field(fieldName: String, fieldValue: String)

sealed trait SearchQuery

case object MatchAllQuery extends SearchQuery

case class MatchQuery(fields: Option[Field] = None) extends SearchQuery
