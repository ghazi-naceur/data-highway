package io.oss.data.highway.sinks

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

  val folderParquetToCsvData = "src/test/resources/parquet_to_csv-data/"
  val folderJsonToCsvData    = "src/test/resources/json_to_csv-data/"
  val folderAvroToCsvData    = "src/test/resources/avro_to_csv-data/"
  val folderXlsxCsvData      = "src/test/resources/xlsx_to_csv-data/"
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

  "CsvSink.convertToCsv" should "save a parquet as a csv file" in {
    CsvSink
      .convertToCsv(
        folderParquetToCsvData + "input/mock-data-2",
        folderParquetToCsvData + "output/mock-data-2",
        folderJsonToCsvData + "processed",
        SaveMode.Overwrite,
        PARQUET
      )
    val actual =
      DataFrameUtils
        .loadDataFrame(folderParquetToCsvData + "output/mock-data-2", CSV)
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  "CsvSink.convertToCsv" should "save a json as a csv file" in {
    CsvSink
      .convertToCsv(
        folderJsonToCsvData + "input/mock-data-2",
        folderJsonToCsvData + "output/mock-data-2",
        folderJsonToCsvData + "processed",
        SaveMode.Overwrite,
        JSON
      )
    val actual =
      DataFrameUtils
        .loadDataFrame(folderJsonToCsvData + "output/mock-data-2", CSV)
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  "CsvSink.convertToCsv" should "save a avro as a csv file" in {
    CsvSink
      .convertToCsv(
        folderAvroToCsvData + "input/mock-data-2",
        folderAvroToCsvData + "output/mock-data-2",
        folderAvroToCsvData + "processed",
        SaveMode.Overwrite,
        AVRO
      )
    val actual =
      DataFrameUtils
        .loadDataFrame(folderAvroToCsvData + "output/mock-data-2", CSV)
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  "CsvSink.convertToCsv" should "save a xlsx as a csv file" in {
    CsvSink
      .convertToCsv(
        folderXlsxCsvData + "input/folder1/mock-xlsx-data-13.xlsx",
        folderXlsxCsvData + "output/folder1/mock-xlsx-data-13",
        folderXlsxCsvData + "processed",
        SaveMode.Overwrite,
        XLSX
      )
    val actual =
      DataFrameUtils
        .loadDataFrame(folderXlsxCsvData + "output/folder1/mock-xlsx-data-13", CSV)
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected2, ignoreNullable = true)
  }
}
