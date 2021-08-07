package io.oss.data.highway.sinks

import io.oss.data.highway.models.{Cassandra, DataType}
import io.oss.data.highway.utils.DataFrameUtils
import org.apache.spark.sql.SaveMode
import cats.implicits._

object CassandraSampler {

  /**
    * Handles the output Cassandra channel
    * @param out The output path
    * @param cassandra The Cassandra configs
    * @param saveMode The output save mode
    * @param dataType The output data type
    * @return a Unit, otherwise Throwable
    */
  def handleCassandraChannel(
      out: String,
      cassandra: Cassandra,
      saveMode: SaveMode,
      dataType: DataType
  ): Either[Throwable, Unit] = {
    DataFrameUtils
      .loadDataFrame("", cassandra)
      .traverse(df => DataFrameUtils.saveDataFrame(df, dataType, out, saveMode))
      .flatten
  }
}
