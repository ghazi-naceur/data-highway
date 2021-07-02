package io.oss.data.highway.z.hdfs.integration.tests

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import io.oss.data.highway.sinks.AvroSink
import io.oss.data.highway.models.{AVRO, CSV, JSON, Local, PARQUET}
import io.oss.data.highway.utils.Constants.SEPARATOR
import io.oss.data.highway.utils.DataFrameUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object AvroConversion extends DatasetComparer {

  val folderParquetToAvro = "src/test/resources/parquet_to_avro-data/"
  val folderJsonToAvro    = "src/test/resources/json_to_avro-data/"
  val folderCsvToAvro     = "src/test/resources/csv_to_avro-data/"

  val hdfsParquetToAvro =
    "hdfs://localhost:9000/data-highway/parquet_to_avro-data/"
  val hdfsJsonToAvro = "hdfs://localhost:9000/data-highway/json_to_avro-data/"
  val hdfsCsvToAvro  = "hdfs://localhost:9000/data-highway/csv_to_avro-data/"

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  // Insert parquet files input
  def insertParquetInputData(): Unit = {
    spark.read
      .format("parquet")
      .load(folderParquetToAvro + "input/mock-data-2")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(hdfsParquetToAvro + "input/mock-data-2")
  }

  // Insert json files input
  def insertJsonInputData(): Unit = {
    spark.read
      .format("json")
      .load(folderJsonToAvro + "input/mock-data-2")
      .write
      .format("json")
      .mode(SaveMode.Overwrite)
      .save(hdfsJsonToAvro + "input/mock-data-2")
  }

  // Insert csv files input
  def insertCsvInputData(): Unit = {
    spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", SEPARATOR)
      .csv(folderCsvToAvro + "input/mock-data-2")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", SEPARATOR)
      .csv(hdfsCsvToAvro + "input/mock-data-2")
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

  def convertParquetToAvro(): Unit = {
    AvroSink
      .convertToAvro(
        hdfsParquetToAvro + "input/mock-data-2",
        hdfsParquetToAvro + "output/mock-data-2",
        hdfsParquetToAvro + "processed",
        SaveMode.Overwrite,
        Local,
        PARQUET
      )
    val actual = DataFrameUtils
      .loadDataFrame(hdfsParquetToAvro + "output/mock-data-2", AVRO)
      .right
      .get
      .orderBy("id")
      .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  def convertJsonToAvro(): Unit = {
    AvroSink
      .convertToAvro(
        hdfsJsonToAvro + "input/mock-data-2",
        hdfsJsonToAvro + "output/mock-data-2",
        hdfsJsonToAvro + "processed",
        SaveMode.Overwrite,
        Local,
        JSON
      )
    val actual = DataFrameUtils
      .loadDataFrame(hdfsJsonToAvro + "output/mock-data-2", AVRO)
      .right
      .get
      .orderBy("id")
      .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  def convertCsvToAvro(): Unit = {
    AvroSink
      .convertToAvro(
        hdfsCsvToAvro + "input/mock-data-2",
        hdfsCsvToAvro + "output/mock-data-2",
        hdfsCsvToAvro + "processed",
        SaveMode.Overwrite,
        Local,
        CSV
      )
    val actual = DataFrameUtils
      .loadDataFrame(hdfsCsvToAvro + "output/mock-data-2", AVRO)
      .right
      .get
      .orderBy("id")
      .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  def main(args: Array[String]): Unit = {
    insertParquetInputData()
    convertParquetToAvro()

    insertJsonInputData()
    convertJsonToAvro()

    insertCsvInputData()
    convertCsvToAvro()
  }
}
