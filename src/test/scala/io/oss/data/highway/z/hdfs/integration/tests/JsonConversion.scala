package io.oss.data.highway.z.hdfs.integration.tests

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import io.oss.data.highway.sinks.JsonSink
import io.oss.data.highway.models.{AVRO, CSV, JSON, Local, PARQUET}
import io.oss.data.highway.utils.Constants.SEPARATOR
import io.oss.data.highway.utils.DataFrameUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object JsonConversion extends DatasetComparer {

  val folderAvroToJson    = "src/test/resources/avro_to_json-data/"
  val folderCsvToJson     = "src/test/resources/csv_to_json-data/"
  val folderParquetToJson = "src/test/resources/parquet_to_json-data/"

  val hdfsAvroToJson =
    "hdfs://localhost:9000/data-highway/avro_to_json-data/"
  val hdfsCsvToJson =
    "hdfs://localhost:9000/data-highway/csv_to_json-data/"
  val hdfsParquetToJson =
    "hdfs://localhost:9000/data-highway/parquet_to_json-data/"

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  // Insert avro files input
  def insertAvroInputData(): Unit = {
    spark.read
      .format("avro")
      .load(folderAvroToJson + "input/mock-data-2")
      .write
      .format("avro")
      .mode(SaveMode.Overwrite)
      .save(hdfsAvroToJson + "input/mock-data-2")
  }

  // Insert csv files input
  def insertCsvInputData(): Unit = {
    spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", SEPARATOR)
      .csv(folderCsvToJson + "input/mock-data-2")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", SEPARATOR)
      .csv(hdfsCsvToJson + "input/mock-data-2")
  }

  // Insert parquet files input
  def insertParquetInputData(): Unit = {
    spark.read
      .format("parquet")
      .load(folderParquetToJson + "input/mock-data-2")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(hdfsParquetToJson + "input/mock-data-2")
  }

  private def getExpected: DataFrame = {
    import spark.implicits._
    List(
      (6.0, "Marquita", "Jarrad", "mjarrad5@rakuten.co.jp", "Female", "247.246.40.151"),
      (7.0, "Bordie", "Altham", "baltham6@hud.gov", "Male", "234.202.91.240"),
      (8.0, "Dom", "Greson", "dgreson7@somehting.com", "Male", "103.7.243.71"),
      (9.0, "Alphard", "Meardon", "ameardon8@comsenz.com", "Male", "37.31.17.200"),
      (10.0, "Reynold", "Neighbour", "rneighbour9@gravatar.com", "Male", "215.57.123.52")
    ).toDF("id", "first_name", "last_name", "email", "gender", "ip_address")
  }

  def convertAvroToJson(): Unit = {
    JsonSink
      .convertToJson(
        hdfsAvroToJson + "input/mock-data-2",
        hdfsAvroToJson + "output/mock-data-2",
        hdfsAvroToJson + "processed",
        SaveMode.Overwrite,
        Local,
        AVRO
      )
    val actual = DataFrameUtils
      .loadDataFrame(hdfsAvroToJson + "output/mock-data-2", JSON)
      .right
      .get
      .orderBy("id")
      .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  def convertCsvToJson(): Unit = {
    JsonSink
      .convertToJson(
        hdfsCsvToJson + "input/mock-data-2",
        hdfsCsvToJson + "output/mock-data-2",
        hdfsCsvToJson + "processed",
        SaveMode.Overwrite,
        Local,
        CSV
      )
    val actual = DataFrameUtils
      .loadDataFrame(hdfsCsvToJson + "output/mock-data-2", JSON)
      .right
      .get
      .orderBy("id")
      .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  def convertParquetToJson(): Unit = {
    JsonSink
      .convertToJson(
        hdfsParquetToJson + "input/mock-data-2",
        hdfsParquetToJson + "output/mock-data-2",
        hdfsParquetToJson + "processed",
        SaveMode.Overwrite,
        Local,
        PARQUET
      )
    val actual = DataFrameUtils
      .loadDataFrame(hdfsParquetToJson + "output/mock-data-2", JSON)
      .right
      .get
      .orderBy("id")
      .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  def main(args: Array[String]): Unit = {
    insertAvroInputData()
    convertAvroToJson()

    insertCsvInputData()
    convertCsvToJson()

    insertParquetInputData()
    convertParquetToJson()
  }
}
