package gn.oss.data.highway.models

sealed trait BoolFilter

case object Must    extends BoolFilter
case object MustNot extends BoolFilter
case object Should  extends BoolFilter
