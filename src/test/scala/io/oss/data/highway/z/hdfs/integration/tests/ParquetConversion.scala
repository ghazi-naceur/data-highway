package io.oss.data.highway.z.hdfs.integration.tests

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import io.oss.data.highway.configuration.SparkConfigs
import io.oss.data.highway.converter.ParquetSink
import io.oss.data.highway.model.{AVRO, CSV, JSON, PARQUET, WARN}
import io.oss.data.highway.utils.Constants.SEPARATOR
import io.oss.data.highway.utils.DataFrameUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ParquetConversion extends DatasetComparer {

  val folderAvroToParquet = "src/test/resources/avro_to_parquet-data/"
  val folderJsonToParquet = "src/test/resources/json_to_parquet-data/"
  val folderCsvToParquet = "src/test/resources/csv_to_parquet-data/"

  val hdfsAvroToParquet =
    "hdfs://localhost:9000/data-highway/avro_to_parquet-data/"
  val hdfsJsonToParquet =
    "hdfs://localhost:9000/data-highway/json_to_parquet-data/"
  val hdfsCsvToParquet =
    "hdfs://localhost:9000/data-highway/csv_to_parquet-data/"

  val sparkConfig: SparkConfigs =
    SparkConfigs("handler-app-test", "local[*]", WARN)

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
      .load(folderAvroToParquet + "input/mock-data-2")
      .write
      .format("avro")
      .mode(SaveMode.Overwrite)
      .save(hdfsAvroToParquet + "input/mock-data-2")
  }

  // Insert json files input
  def insertJsonInputData(): Unit = {
    spark.read
      .format("json")
      .load(folderJsonToParquet + "input/mock-data-2")
      .write
      .format("json")
      .mode(SaveMode.Overwrite)
      .save(hdfsJsonToParquet + "input/mock-data-2")
  }

  // Insert csv files input
  def insertCsvInputData(): Unit = {
    spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", SEPARATOR)
      .csv(folderCsvToParquet + "input/mock-data-2")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", SEPARATOR)
      .csv(hdfsCsvToParquet + "input/mock-data-2")
  }

  private def getExpected: DataFrame = {
    import spark.implicits._
    List(
      (6.0,
       "Marquita",
       "Jarrad",
       "mjarrad5@rakuten.co.jp",
       "Female",
       "247.246.40.151"),
      (7.0, "Bordie", "Altham", "baltham6@hud.gov", "Male", "234.202.91.240"),
      (8.0, "Dom", "Greson", "dgreson7@somehting.com", "Male", "103.7.243.71"),
      (9.0,
       "Alphard",
       "Meardon",
       "ameardon8@comsenz.com",
       "Male",
       "37.31.17.200"),
      (10.0,
       "Reynold",
       "Neighbour",
       "rneighbour9@gravatar.com",
       "Male",
       "215.57.123.52")
    ).toDF("id", "first_name", "last_name", "email", "gender", "ip_address")
  }

  def convertAvroToParquet(): Unit = {
    ParquetSink
      .convertToParquet(hdfsAvroToParquet + "input/mock-data-2",
                        hdfsAvroToParquet + "output/mock-data-2",
                        SaveMode.Overwrite,
                        AVRO,
                        sparkConfig)
    val actual = DataFrameUtils(sparkConfig)
      .loadDataFrame(hdfsAvroToParquet + "output/mock-data-2", PARQUET)
      .right
      .get
      .orderBy("id")
      .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  def convertJsonToParquet(): Unit = {
    ParquetSink
      .convertToParquet(hdfsJsonToParquet + "input/mock-data-2",
                        hdfsJsonToParquet + "output/mock-data-2",
                        SaveMode.Overwrite,
                        JSON,
                        sparkConfig)
    val actual = DataFrameUtils(sparkConfig)
      .loadDataFrame(hdfsJsonToParquet + "output/mock-data-2", PARQUET)
      .right
      .get
      .orderBy("id")
      .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  def convertCsvToParquet(): Unit = {
    ParquetSink
      .convertToParquet(hdfsCsvToParquet + "input/mock-data-2",
                        hdfsCsvToParquet + "output/mock-data-2",
                        SaveMode.Overwrite,
                        CSV,
                        sparkConfig)
    val actual = DataFrameUtils(sparkConfig)
      .loadDataFrame(hdfsCsvToParquet + "output/mock-data-2", PARQUET)
      .right
      .get
      .orderBy("id")
      .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  def main(args: Array[String]): Unit = {
    insertAvroInputData()
    convertAvroToParquet()

    insertJsonInputData()
    convertJsonToParquet()

    insertCsvInputData()
    convertCsvToParquet()
  }
}
