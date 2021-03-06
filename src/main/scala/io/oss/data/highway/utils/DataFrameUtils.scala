package io.oss.data.highway.utils

import org.apache.spark.sql.DataFrame
import cats.syntax.either._
import io.oss.data.highway.models.{AVRO, CSV, DataType, JSON, PARQUET}
import io.oss.data.highway.utils.Constants.SEPARATOR

object DataFrameUtils extends SparkUtils {

  /**
    * Loads a dataframe
    * @param in The input path
    * @param dataType a datatype to be load : CSV, JSON, PARQUET or AVRO
    * @return A DataFrame, otherwise an Error
    */
  def loadDataFrame(in: String,
                    dataType: DataType): Either[Throwable, DataFrame] = {
    Either.catchNonFatal {
      dataType match {
        case JSON =>
          sparkSession.read
            .json(in)
        case CSV =>
          sparkSession.read
            .option("inferSchema", "true")
            .option("header", "true")
            .option("sep", SEPARATOR)
            .csv(in)
        case PARQUET =>
          sparkSession.read
            .parquet(in)
        case AVRO =>
          sparkSession.read
            .format(AVRO.extension)
            .load(in)
        case _ =>
          throw new RuntimeException("This mode is not supported when defining input data types. The supported Kafka Consume Mode are : " +
            s"'${JSON.getClass.getName}', '${CSV.getClass.getName}', '${PARQUET.getClass.getName}' and '${AVRO.getClass.getName}'.")
      }
    }
  }
}
