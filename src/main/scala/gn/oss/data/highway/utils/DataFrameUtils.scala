package gn.oss.data.highway.utils

import org.apache.spark.sql.{DataFrame, SaveMode}
import cats.syntax.either._
import gn.oss.data.highway.models.{
  AVRO,
  CSV,
  CassandraDB,
  Compression,
  DataType,
  JSON,
  ORC,
  OrcCompression,
  PARQUET,
  ParquetCompression,
  PostgresDB,
  XLSX
}
import gn.oss.data.highway.configs.{PostgresUtils, SparkUtils}

import java.util.UUID

object DataFrameUtils extends SparkUtils with PostgresUtils {

  /**
    * Loads a dataframe
    *
    * @param in The input path
    * @param dataType a datatype to be load : CSV, JSON, PARQUET, AVRO, Cassandra or XLSX
    * @return A DataFrame, otherwise a Throwable
    */
  def loadDataFrame(dataType: DataType, in: String): Either[Throwable, DataFrame] = {
    dataType match {
      case JSON                                => loadJson(in)
      case CSV(inferSchema, header, separator) => loadCsv(in, inferSchema, header, separator)
      case PARQUET(_)                          => loadParquet(in)
      case ORC(_)                              => loadOrc(in)
      case AVRO                                => loadAvro(in)
      case XLSX                                => loadXlsx(in)
      case CassandraDB(keyspace, table)        => loadFromCassandra(keyspace, table)
      case PostgresDB(database, table)         => loadFromPostgres(database, table)
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
  def saveDataFrame(df: DataFrame, dataType: DataType, out: String, saveMode: SaveMode): Either[Throwable, Unit] = {
    dataType match {
      case JSON                                => saveJson(df, out, saveMode)
      case CSV(inferSchema, header, separator) => saveCsv(df, out, saveMode, inferSchema, header, separator)
      case PARQUET(compression)                => saveParquet(df, out, saveMode, compression)
      case ORC(compression)                    => saveOrc(df, out, saveMode, compression)
      case AVRO                                => saveAvro(df, out, saveMode)
      case XLSX                                => saveXlsx(df, out, saveMode)
      case CassandraDB(keyspace, table)        => saveInCassandra(df, saveMode, keyspace, table)
      case PostgresDB(database, table)         => saveInPostgres(df, saveMode, database, table)
    }
  }

  private def loadFromPostgres(database: String, table: String): Either[Throwable, DataFrame] = {
    Either.catchNonFatal {
      sparkSession.read
        .format("jdbc")
        .option("url", s"${postgresConf.host}:${postgresConf.port}/$database")
        .option("dbtable", table) // can be "tablename" or "schema.tablename"
        .option("user", postgresConf.user)
        .option("password", postgresConf.password)
        .load()
    }
  }

  private def loadFromCassandra(keyspace: String, table: String): Either[Throwable, DataFrame] = {
    Either.catchNonFatal {
      sparkSession.read
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", keyspace)
        .option("table", table)
        .load()
    }
  }

  private def loadXlsx(in: String): Either[Throwable, DataFrame] = {
    Either.catchNonFatal {
      sparkSession.read
        .format("com.crealytics.spark.excel")
        .option("header", "true")
        .option("treatEmptyValuesAsNulls", "true")
        .option("inferSchema", "true")
        .load(in)
    }
  }

  private def loadAvro(in: String): Either[Throwable, DataFrame] = {
    Either.catchNonFatal {
      sparkSession.read
        .format(AVRO.extension)
        .load(in)
    }
  }

  private def loadOrc(in: String): Either[Throwable, DataFrame] = {
    Either.catchNonFatal {
      sparkSession.read
        .orc(in)
    }
  }

  private def loadParquet(in: String): Either[Throwable, DataFrame] = {
    Either.catchNonFatal {
      sparkSession.read
        .parquet(in)
    }
  }

  private def loadCsv(
    in: String,
    inferSchema: Boolean,
    header: Boolean,
    separator: String
  ): Either[Throwable, DataFrame] = {
    Either.catchNonFatal {
      sparkSession.read
        .option("inferSchema", inferSchema)
        .option("header", header)
        .option("sep", separator)
        .csv(in)
    }
  }

  private def loadJson(in: String): Either[Throwable, DataFrame] = {
    Either.catchNonFatal {
      sparkSession.read
        .json(in)
    }
  }

  private def saveInPostgres(
    df: DataFrame,
    saveMode: SaveMode,
    database: String,
    table: String
  ): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      df.write
        .format("jdbc")
        .option("url", s"${postgresConf.host}:${postgresConf.port}/$database")
        .option("dbtable", table) // can be "tablename" or "schema.tablename"
        .option("user", postgresConf.user)
        .option("password", postgresConf.password)
        .mode(saveMode)
        .save()
    }
  }

  private def saveInCassandra(
    df: DataFrame,
    saveMode: SaveMode,
    keyspace: String,
    table: String
  ): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      df.write
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", keyspace)
        .option("table", table)
        .mode(saveMode)
        .save()
    }
  }

  private def saveXlsx(df: DataFrame, out: String, saveMode: SaveMode): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      df.write
        .format("com.crealytics.spark.excel")
        .option("dataAddress", "'My Sheet'!A1:Z1000000")
        .option("header", "true")
        .option("dateFormat", "yy-mmm-d")
        .mode(saveMode)
        .save(s"$out/generated_xlsx-${UUID.randomUUID().toString}-${System.currentTimeMillis().toString}.xlsx")
    }
  }

  private def saveAvro(df: DataFrame, out: String, saveMode: SaveMode): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      df.write
        .format(AVRO.extension)
        .mode(saveMode)
        .save(out)
    }
  }

  private def saveOrc(
    df: DataFrame,
    out: String,
    saveMode: SaveMode,
    compression: Option[OrcCompression]
  ): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      val computedCompression = computeCompression(compression)
      df.write
        .mode(saveMode)
        .option("compression", computedCompression.value)
        .orc(out)
    }
  }

  private def saveParquet(
    df: DataFrame,
    out: String,
    saveMode: SaveMode,
    compression: Option[ParquetCompression]
  ): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      val computedCompression = computeCompression(compression)
      df.write
        .mode(saveMode)
        .option("compression", computedCompression.value)
        .parquet(out)
    }
  }

  private def saveCsv(
    df: DataFrame,
    out: String,
    saveMode: SaveMode,
    inferSchema: Boolean,
    header: Boolean,
    separator: String
  ): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      df.coalesce(1)
        .write
        .mode(saveMode)
        .option("inferSchema", inferSchema)
        .option("header", header)
        .option("sep", separator)
        .csv(out)
    }
  }

  private def saveJson(df: DataFrame, out: String, saveMode: SaveMode): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      df.coalesce(1)
        .write
        .mode(saveMode)
        .json(out)
    }
  }

  private def computeCompression(compression: Option[Compression]): Compression = {
    if (compression.isDefined)
      compression.get
    else
      gn.oss.data.highway.models.None
  }

  /**
    * Converts elements to JSON string
    *
    * @param element The element to be converted
    * @return Json String
    */
  @annotation.nowarn
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
    * @param dataframe The dataframe to be converted
    * @return a list of Json lines, otherwise a Throwable
    */
  def convertDataFrameToJsonLines(dataframe: DataFrame): Either[Throwable, List[String]] = {
    Either.catchNonFatal {
      import scala.collection.JavaConverters._
      import DataFrameUtils.sparkSession.implicits._
      val fieldNames = dataframe.head().schema.fieldNames
      dataframe
        .map(row => {
          val rowAsMap = row.getValuesMap(fieldNames)
          DataFrameUtils.toJson(rowAsMap)
        })
        .collectAsList()
        .asScala
        .toList
    }
  }
}
