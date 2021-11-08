package gn.oss.data.highway.models

import org.apache.spark.sql.SaveMode

// todo check if SaveMode/Enum is supported by pure config.. if so, delete Consistency class
sealed trait Consistency {
  def toSaveMode: SaveMode
}

object Consistency {
  def toConsistency(saveMode: SaveMode): Consistency =
    saveMode match {
      case SaveMode.Append        => Append
      case SaveMode.Overwrite     => Overwrite
      case SaveMode.ErrorIfExists => ErrorIfExists
      case SaveMode.Ignore        => Ignore
    }
}

case object Overwrite extends Consistency {
  override def toSaveMode: SaveMode = SaveMode.Overwrite
}
case object Append extends Consistency {
  override def toSaveMode: SaveMode = SaveMode.Append
}
case object ErrorIfExists extends Consistency {
  override def toSaveMode: SaveMode = SaveMode.ErrorIfExists
}
case object Ignore extends Consistency {
  override def toSaveMode: SaveMode = SaveMode.Ignore
}
