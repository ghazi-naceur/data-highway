package gn.oss.data.highway.utils

import org.apache.spark.sql.{DataFrame, SaveMode}
import cats.syntax.either._
import gn.oss.data.highway.models.{AVRO, CSV, CassandraDB, DataType, JSON, PARQUET, XLSX}
import Constants.SEPARATOR

import java.util.UUID

object DataFrameUtils extends SparkUtils {

  /**
    * Loads a dataframe
    *
    * @param in The input path
    * @param dataType a datatype to be load : CSV, JSON, PARQUET, AVRO, Cassandra or XLSX
    * @return A DataFrame, otherwise a Throwable
    */
  def loadDataFrame(dataType: DataType, in: String): Either[Throwable, DataFrame] = {
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
        case XLSX =>
          sparkSession.read
            .format("com.crealytics.spark.excel")
            .option("header", "true")
            .option("treatEmptyValuesAsNulls", "true")
            .option("inferSchema", "true")
            .load(in)
        case CassandraDB(keyspace, table) =>
          sparkSession.read
            .format("org.apache.spark.sql.cassandra")
            .option("keyspace", keyspace)
            .option("table", table)
            .load()
        case _ =>
          throw new RuntimeException(
            "This mode is not supported when defining input data types. The supported Kafka Consume Mode are : " +
              s"'${JSON.getClass.getName}', '${CSV.getClass.getName}', '${PARQUET.getClass.getName}' and '${AVRO.getClass.getName}'."
          )
      }
    }
  }

  /**
    * Saves a dataframe
    *
    * @param df Dataframe to be saved
    * @param dataType a datatype to be load : CSV, JSON, PARQUET, AVRO, Cassandra or XLSX
    * @param out The output path
    * @param saveMode The output save mode
    * @return a Unit, otherwise a Throwable
    */
  def saveDataFrame(
      df: DataFrame,
      dataType: DataType,
      out: String,
      saveMode: SaveMode
  ): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      dataType match {
        case JSON =>
          df.coalesce(1)
            .write
            .mode(saveMode)
            .json(out)
        case CSV =>
          df.coalesce(1)
            .write
            .mode(saveMode)
            .option("inferSchema", "true")
            .option("header", "true")
            .option("sep", SEPARATOR)
            .csv(out)
        case PARQUET =>
          df.write
            .mode(saveMode)
            .parquet(out)
        case AVRO =>
          df.write
            .format(AVRO.extension)
            .mode(saveMode)
            .save(out)
        case XLSX =>
          df.write
            .format("com.crealytics.spark.excel")
            .option("dataAddress", "'My Sheet'!A1:Z1000000")
            .option("header", "true")
            .option("dateFormat", "yy-mmm-d")
            .mode(saveMode)
            .save(
              s"$out/generated_xlsx-${UUID.randomUUID().toString}-${System.currentTimeMillis().toString}.xlsx"
            )
        case CassandraDB(keyspace, table) =>
          df.write
            .format("org.apache.spark.sql.cassandra")
            .option("keyspace", keyspace)
            .option("table", table)
            .mode(saveMode)
            .save()
      }
    }
  }

  /**
    * Converts elements to JSON string
    *
    * @param element The element to be converted
    * @return Json String
    */
  private def toJson(element: Any): String =
    element match {
      case mapElem: Map[String, Any] => s"{${mapElem.map(toJson(_)).mkString(",")}}"
      case tupleElem: (String, Any)  => s""""${tupleElem._1}":${toJson(tupleElem._2)}"""
      case seqElem: Seq[Any]         => s"""[${seqElem.map(toJson).mkString(",")}]"""
      case stringElem: String        => s""""$stringElem""""
      case null                      => "null"
      case _                         => element.toString
    }

  /**
    * Converts a dataframe to a list of json lines
    *
    * @param df The dataframe to be converted
    * @return a list of Json lines, otherwise a Throwable
    */
  def convertDataFrameToJsonLines(df: DataFrame): Either[Throwable, List[String]] = {
    Either.catchNonFatal {
      import scala.collection.JavaConverters._
      import DataFrameUtils.sparkSession.implicits._
      val fieldNames = df.head().schema.fieldNames
      df.map(row => {
          val rowAsMap = row.getValuesMap(fieldNames)
          DataFrameUtils.toJson(rowAsMap)
        })
        .collectAsList()
        .asScala
        .toList
    }
  }
}