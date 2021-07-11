package io.oss.data.highway.z.hdfs.integration.tests

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import io.oss.data.highway.sinks.CsvSink
import io.oss.data.highway.models.{AVRO, CSV, JSON, Local, PARQUET}
import io.oss.data.highway.utils.DataFrameUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object CsvConversion extends DatasetComparer {

  val folderAvroToCsv    = "src/test/resources/avro_to_csv-data/"
  val folderJsonToCsv    = "src/test/resources/json_to_csv-data/"
  val folderParquetToCsv = "src/test/resources/parquet_to_csv-data/"

  val hdfsAvroToCsv =
    "hdfs://localhost:9000/data-highway/avro_to_csv-data/"
  val hdfsJsonToCsv =
    "hdfs://localhost:9000/data-highway/json_to_csv-data/"
  val hdfsParquetToCsv =
    "hdfs://localhost:9000/data-highway/parquet_to_csv-data/"

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
      .load(folderAvroToCsv + "input/mock-data-2")
      .write
      .format("avro")
      .mode(SaveMode.Overwrite)
      .save(hdfsAvroToCsv + "input/mock-data-2")
  }

  // Insert json files input
  def insertJsonInputData(): Unit = {
    spark.read
      .format("json")
      .load(folderJsonToCsv + "input/mock-data-2")
      .write
      .format("json")
      .mode(SaveMode.Overwrite)
      .save(hdfsJsonToCsv + "input/mock-data-2")
  }

  // Insert parquet files input
  def insertParquetInputData(): Unit = {
    spark.read
      .format("parquet")
      .load(folderParquetToCsv + "input/mock-data-2")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(hdfsParquetToCsv + "input/mock-data-2")
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

  def convertAvroToCsv(): Unit = {
    CsvSink
      .convertToCsv(
        hdfsAvroToCsv + "input/mock-data-2",
        hdfsAvroToCsv + "output/mock-data-2",
        hdfsAvroToCsv + "processed",
        SaveMode.Overwrite,
        AVRO
      )
    val actual = DataFrameUtils
      .loadDataFrame(hdfsAvroToCsv + "output/mock-data-2", CSV)
      .right
      .get
      .orderBy("id")
      .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  def convertJsonToCsv(): Unit = {
    CsvSink
      .convertToCsv(
        hdfsJsonToCsv + "input/mock-data-2",
        hdfsJsonToCsv + "output/mock-data-2",
        hdfsAvroToCsv + "processed",
        SaveMode.Overwrite,
        JSON
      )
    val actual = DataFrameUtils
      .loadDataFrame(hdfsJsonToCsv + "output/mock-data-2", CSV)
      .right
      .get
      .orderBy("id")
      .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  def convertParquetToCsv(): Unit = {
    CsvSink
      .convertToCsv(
        hdfsParquetToCsv + "input/mock-data-2",
        hdfsParquetToCsv + "output/mock-data-2",
        hdfsAvroToCsv + "processed",
        SaveMode.Overwrite,
        PARQUET
      )
    val actual = DataFrameUtils
      .loadDataFrame(hdfsParquetToCsv + "output/mock-data-2", CSV)
      .right
      .get
      .orderBy("id")
      .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  def main(args: Array[String]): Unit = {
    insertAvroInputData()
    convertAvroToCsv()

    insertJsonInputData()
    convertJsonToCsv()

    insertParquetInputData()
    convertParquetToCsv()
  }
}
