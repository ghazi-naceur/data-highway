package io.oss.data.highway.sinks

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Paths}
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import io.oss.data.highway.models.{AVRO, CSV, JSON, Local, PARQUET}
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

  "CsvSink.saveParquetAsCsv" should "save a parquet as a csv file" in {
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

  "CsvSink.saveJsonAsCsv" should "save a json as a csv file" in {
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

  "CsvSink.saveAvroAsCsv" should "save a avro as a csv file" in {
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

  "CsvSink.convertXlsxFileToCsvFiles" should "convert xlsx file to multiple csv files" in {
    val inputStream =
      new FileInputStream(folderXlsxCsvData + "input/folder3/mock-xlsx-data-1.xlsx")
    CsvSink.convertXlsxFileToCsvFiles(
      "folder3/mock-xlsx-data-1",
      inputStream,
      folderXlsxCsvData + "output/"
    )
    val d = new File(folderXlsxCsvData + "output/folder3/mock-xlsx-data-1")
    d.listFiles
      .map(file => file.getName)
      .toList should contain allElementsOf List(
      "data1.csv",
      "data2.csv",
      "data3.csv",
      "data4.csv",
      "data5.csv"
    )
  }

  "CsvSink.apply" should "convert xlsx files to multiple csv files" in {
    CsvSink.handleXlsxCsvChannel(
      folderXlsxCsvData + "input/",
      folderXlsxCsvData + "output/",
      Seq("xlsx", "xls")
    )
    val list1 = List(
      "data1.csv",
      "data2.csv",
      "data3.csv",
      "data4.csv",
      "data5.csv"
    )
    val list2 = List(
      "data6.csv",
      "data7.csv",
      "data8.csv",
      "data9.csv",
      "data10.csv"
    )

    val dir1 = new File(folderXlsxCsvData + "output/folder1/mock-xlsx-data-11")
    dir1.listFiles
      .map(file => file.getName)
      .toList should contain allElementsOf list1

    val dir2 = new File(folderXlsxCsvData + "output/folder1/mock-xlsx-data-12")
    dir2.listFiles
      .map(file => file.getName)
      .toList should contain allElementsOf list2

    val dir3 = new File(folderXlsxCsvData + "output/folder2/mock-xlsx-data-21")
    dir3.listFiles
      .map(file => file.getName)
      .toList should contain allElementsOf list1

    val dir4 = new File(folderXlsxCsvData + "output/folder2/mock-xlsx-data-22")
    dir4.listFiles
      .map(file => file.getName)
      .toList should contain allElementsOf list2

    val dir5 =
      new File(folderXlsxCsvData + "output/folder3/mock-xlsx-data-1")
    dir5.listFiles
      .map(file => file.getName)
      .toList should contain allElementsOf list1

    val dir6 =
      new File(folderXlsxCsvData + "output/folder3/mock-xlsx-data-2")
    dir6.listFiles
      .map(file => file.getName)
      .toList should contain allElementsOf list2
  }

  "CsvSink.createPathRecursively" should "create a directory and its subdirectories" in {
    val str =
      CsvSink.createPathRecursively("src/test/resources/xlsx_to_csv-data/output/sub1/sub2/sub3")
    Files.exists(Paths.get(str)) shouldBe true
    beforeEach()
  }
}
