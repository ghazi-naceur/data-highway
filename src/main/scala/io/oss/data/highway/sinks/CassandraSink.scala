package io.oss.data.highway.sinks

import io.oss.data.highway.models.{CSV, Storage}
import io.oss.data.highway.utils.{DataFrameUtils, HdfsUtils}
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode

object CassandraSink extends HdfsUtils {

  val logger: Logger = Logger.getLogger(CassandraSink.getClass.getName)

  /**
    * Inserts csv file content into Cassandra
    *
    * @param inputPath The CSV input path
    * @param keyspace The output Cassandra keyspace
    * @param table The output Cassandra table
    * @param saveMode The save mode
    * @return a Unit, otherwise a CassandraError
    */
  def insert(
      inputPath: String,
      keyspace: String,
      table: String,
      saveMode: SaveMode
  ): Either[Throwable, Unit] = {
    DataFrameUtils
      .loadDataFrame(inputPath, CSV)
      .map(df => {
        df.write
          .format("org.apache.spark.sql.cassandra")
          .option("keyspace", keyspace)
          .option("table", table)
          .mode(saveMode)
          .save()
      })
  }
}
