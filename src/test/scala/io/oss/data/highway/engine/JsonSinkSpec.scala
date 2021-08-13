package io.oss.data.highway.engine

import java.io.File
import java.nio.file.{Files, Paths}
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import io.oss.data.highway.models.{AVRO, CSV, JSON, PARQUET}
import io.oss.data.highway.utils.DataFrameUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.reflect.io.Directory

class JsonSinkSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach with DatasetComparer {

  val folderParquetToJson = "src/test/resources/data/parquet/"
  val folderCsvToJson     = "src/test/resources/data/csv/"
  val folderAvroToJson    = "src/test/resources/data/avro/"
  val getExpected: DataFrame = {
    import spark.implicits._
    List(
      (6.0, "Marquita", "Jarrad", "mjarrad5@rakuten.co.jp", "Female", "247.246.40.151"),
      (7.0, "Bordie", "Altham", "baltham6@hud.gov", "Male", "234.202.91.240"),
      (8.0, "Dom", "Greson", "dgreson7@somehting.com", "Male", "103.7.243.71"),
      (9.0, "Alphard", "Meardon", "ameardon8@comsenz.com", "Male", "37.31.17.200"),
      (10.0, "Reynold", "Neighbour", "rneighbour9@gravatar.com", "Male", "215.57.123.52")
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
        val path      = Paths.get(file.getPath)
        val directory = new Directory(file)
        directory.deleteRecursively()
        Files.deleteIfExists(path)
      })
  }

  "BasicSink.convert" should "save a parquet as a json file" in {
    BasicSink.convert(
      PARQUET,
      folderParquetToJson + "input/mock-data-2",
      JSON,
      folderParquetToJson + "output/mock-data-2",
      SaveMode.Overwrite
    )
    val actual =
      DataFrameUtils
        .loadDataFrame(JSON, folderParquetToJson + "output/mock-data-2")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  "BasicSink.convert" should "save an avro as a json file" in {
    BasicSink.convert(
      AVRO,
      folderAvroToJson + "input/mock-data-2",
      JSON,
      folderAvroToJson + "output/mock-data-2",
      SaveMode.Overwrite
    )
    val actual =
      DataFrameUtils
        .loadDataFrame(JSON, folderAvroToJson + "output/mock-data-2")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  "BasicSink.convert" should "save a csv as a json file" in {
    BasicSink.convert(
      CSV,
      folderCsvToJson + "input/mock-data-2",
      JSON,
      folderCsvToJson + "output/mock-data-2",
      SaveMode.Overwrite
    )
    val actual =
      DataFrameUtils
        .loadDataFrame(JSON, folderCsvToJson + "output/mock-data-2")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }
}
