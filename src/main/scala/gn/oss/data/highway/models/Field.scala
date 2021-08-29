package gn.oss.data.highway.models

case class Field(name: String, value: String)
case class LikeFields(fields: List[String], likeTexts: List[String])
case class FieldValues(name: String, values: List[String])
case class Prefix(fieldName: String, value: String)

sealed trait RangeType {
  val value: String
}

case object IntegerRange extends RangeType {
  override val value: String = "integer"
}
case object FloatRange extends RangeType {
  override val value: String = "float"
}
case object LongRange extends RangeType {
  override val value: String = "long"
}
case object StringRange extends RangeType {
  override val value: String = "string"
}

case class RangeField(
    rangeType: RangeType,
    name: String,
    lte: Option[String],
    gte: Option[String]
)

sealed trait GenericRangeField {
  val name: String
  val lte: Option[Any]
  val gte: Option[Any]
}

object GenericRangeField {
  def computeTypedRangeField(range: RangeField): GenericRangeField = {
    range.rangeType match {
      case IntegerRange =>
        IntRangeField(range.name, range.lte.map(_.toInt), range.gte.map(_.toInt))
      case FloatRange =>
        DoubleRangeField(range.name, range.lte.map(_.toDouble), range.gte.map(_.toDouble))
      case LongRange =>
        LongRangeField(range.name, range.lte.map(_.toLong), range.gte.map(_.toLong))
      case StringRange => StringRangeField(range.name, range.lte, range.gte)
    }
  }
}

case class IntRangeField(name: String, lte: Option[Int], gte: Option[Int]) extends GenericRangeField

case class LongRangeField(name: String, lte: Option[Long], gte: Option[Long])
    extends GenericRangeField

case class DoubleRangeField(name: String, lte: Option[Double], gte: Option[Double])
    extends GenericRangeField

case class StringRangeField(name: String, lte: Option[String], gte: Option[String])
    extends GenericRangeField
