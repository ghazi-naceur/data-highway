package io.oss.data.highway.engine

import io.oss.data.highway.models.{Cassandra, CassandraDB, DataType}
import io.oss.data.highway.utils.{Constants, DataFrameUtils}
import org.apache.spark.sql.SaveMode
import cats.implicits._

object CassandraSampler {

  /**
    * Extracts rows from Cassandra and save them into files
    *
    * @param input The input Cassandra entity
    * @param outputDataType The output data type
    * @param outputPath The output data path
    * @param saveMode The output save mode
    * @return a Unit, otherwise Throwable
    */
  def extractRows(
      input: Cassandra,
      outputDataType: DataType,
      outputPath: String,
      saveMode: SaveMode
  ): Either[Throwable, Unit] = {
    DataFrameUtils
      .loadDataFrame(CassandraDB(input.keyspace, input.table), Constants.EMPTY)
      .traverse(df => DataFrameUtils.saveDataFrame(df, outputDataType, outputPath, saveMode))
      .flatten
  }
}
