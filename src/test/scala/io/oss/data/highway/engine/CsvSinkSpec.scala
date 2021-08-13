package io.oss.data.highway.engine

import java.io.File
import java.nio.file.{Files, Paths}
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import io.oss.data.highway.models.{AVRO, CSV, JSON, PARQUET, XLSX}
import io.oss.data.highway.utils.DataFrameUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.reflect.io.Directory

class CsvSinkSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach with DatasetComparer {

  val folderParquetToCsvData = "src/test/resources/data/parquet/"
  val folderJsonToCsvData    = "src/test/resources/data/json/"
  val folderAvroToCsvData    = "src/test/resources/data/avro/"
  val folderXlsxCsvData      = "src/test/resources/data/xlsx/"
  val extensions             = Seq("xlsx", "xls")
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
  val getExpected2: DataFrame = {
    import spark.implicits._
    List(
      (1.0, "Cass", "Roux", "croux0@flavors.me", "Male", "97.27.35.122"),
      (2.0, "Tabbitha", "Richfield", "trichfield1@imageshack.us", "Female", "133.61.220.171"),
      (3.0, "Patrizia", "Dwane", "pdwane2@multiply.com", "Female", "53.91.92.116")
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
    deleteFolderWithItsContent(folderParquetToCsvData)
    deleteFolderWithItsContent(folderJsonToCsvData)
    deleteFolderWithItsContent(folderAvroToCsvData)
    deleteFolderWithItsContent(folderXlsxCsvData)
  }

  private def deleteFolderWithItsContent(path: String): Unit = {
    new File(path + "output").listFiles.toList
      .filterNot(_.getName.endsWith(".gitkeep"))
      .foreach(file => {
        val path      = Paths.get(file.getPath)
        val directory = new Directory(file)
        directory.deleteRecursively()
        Files.deleteIfExists(path)
      })
  }

  "BasicSink.convert" should "save a parquet as a csv file" in {
    BasicSink.convert(
      PARQUET,
      folderParquetToCsvData + "input/mock-data-2",
      CSV,
      folderParquetToCsvData + "output/mock-data-2",
      SaveMode.Overwrite
    )
    val actual =
      DataFrameUtils
        .loadDataFrame(CSV, folderParquetToCsvData + "output/mock-data-2")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  "BasicSink.convert" should "save a json as a csv file" in {
    BasicSink.convert(
      JSON,
      folderJsonToCsvData + "input/mock-data-2",
      CSV,
      folderJsonToCsvData + "output/mock-data-2",
      SaveMode.Overwrite
    )
    val actual =
      DataFrameUtils
        .loadDataFrame(CSV, folderJsonToCsvData + "output/mock-data-2")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  "BasicSink.convert" should "save a avro as a csv file" in {
    BasicSink.convert(
      AVRO,
      folderAvroToCsvData + "input/mock-data-2",
      CSV,
      folderAvroToCsvData + "output/mock-data-2",
      SaveMode.Overwrite
    )
    val actual =
      DataFrameUtils
        .loadDataFrame(CSV, folderAvroToCsvData + "output/mock-data-2")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  "BasicSink.convert" should "save a xlsx as a csv file" in {
    BasicSink.convert(
      XLSX,
      folderXlsxCsvData + "input/folder1/mock-xlsx-data-13.xlsx",
      CSV,
      folderXlsxCsvData + "output/folder1/mock-xlsx-data-13",
      SaveMode.Overwrite
    )
    val actual =
      DataFrameUtils
        .loadDataFrame(CSV, folderXlsxCsvData + "output/folder1/mock-xlsx-data-13")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected2, ignoreNullable = true)
  }
}
