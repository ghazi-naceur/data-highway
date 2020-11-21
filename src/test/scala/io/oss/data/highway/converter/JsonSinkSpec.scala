package io.oss.data.highway.converter

import java.io.File
import java.nio.file.{Files, Paths}

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import io.oss.data.highway.configuration.SparkConfigs
import io.oss.data.highway.model.{AVRO, CSV, JSON, PARQUET, WARN}
import io.oss.data.highway.utils.DataFrameUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.reflect.io.Directory

class JsonSinkSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterEach
    with DatasetComparer {

  val folderParquetToJson = "src/test/resources/parquet_to_json-data/"
  val folderCsvToJson = "src/test/resources/csv_to_json-data/"
  val folderAvroToJson = "src/test/resources/avro_to_json-data/"
  val sparkConfig: SparkConfigs =
    SparkConfigs("handler-app-test", "local[*]", WARN)
  val getExpected: DataFrame = {
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
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  override def beforeEach(): Unit = {
    deleteFoldersWithContents(folderParquetToJson)
    deleteFoldersWithContents(folderAvroToJson)
    deleteFoldersWithContents(folderCsvToJson)
  }

  private def deleteFoldersWithContents(path: String): Unit = {
    new File(path + "output").listFiles.toList
      .filterNot(_.getName.endsWith(".gitkeep"))
      .foreach(file => {
        val path = Paths.get(file.getPath)
        val directory = new Directory(file)
        directory.deleteRecursively()
        Files.deleteIfExists(path)
      })
  }

  "JsonSink.saveParquetAsJson" should "save a parquet as a json file" in {
    JsonSink
      .convertToJson(folderParquetToJson + "input/mock-data-2",
                     folderParquetToJson + "output/mock-data-2",
                     SaveMode.Overwrite,
                     PARQUET,
                     sparkConfig)
    val actual =
      DataFrameUtils(sparkConfig)
        .loadDataFrame(folderParquetToJson + "output/mock-data-2", JSON)
        .right
        .get
        .orderBy("id")
        .select("id",
                "first_name",
                "last_name",
                "email",
                "gender",
                "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  "JsonSink.saveAvroAsJson" should "save an avro as a json file" in {
    JsonSink
      .convertToJson(folderAvroToJson + "input/mock-data-2",
                     folderAvroToJson + "output/mock-data-2",
                     SaveMode.Overwrite,
                     AVRO,
                     sparkConfig)
    val actual =
      DataFrameUtils(sparkConfig)
        .loadDataFrame(folderAvroToJson + "output/mock-data-2", JSON)
        .right
        .get
        .orderBy("id")
        .select("id",
                "first_name",
                "last_name",
                "email",
                "gender",
                "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  "JsonSink.saveCsvAsJson" should "save a csv as a json file" in {
    JsonSink
      .convertToJson(folderCsvToJson + "input/mock-data-2",
                     folderCsvToJson + "output/mock-data-2",
                     SaveMode.Overwrite,
                     CSV,
                     sparkConfig)
    val actual =
      DataFrameUtils(sparkConfig)
        .loadDataFrame(folderCsvToJson + "output/mock-data-2", JSON)
        .right
        .get
        .orderBy("id")
        .select("id",
                "first_name",
                "last_name",
                "email",
                "gender",
                "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }
}
