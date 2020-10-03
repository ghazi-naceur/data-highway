package io.oss.data.highway.converter

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Paths}

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import io.oss.data.highway.configuration.SparkConfig
import io.oss.data.highway.model.{WARN, XlsxCsv}
import io.oss.data.highway.utils.Constants.XLSX_EXTENSION
import io.oss.data.highway.utils.{Constants, MockSheetCreator}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.reflect.io.Directory

class CsvSinkSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterEach
    with DatasetComparer {

  val folderParquetToCsvData = "src/test/resources/parquet_to_csv-data/"
  val folderJsonToCsvData = "src/test/resources/json_to_csv-data/"
  val folderAvroToCsvData = "src/test/resources/avro_to_csv-data/"
  val folderXlsxCsvData = "src/test/resources/xlsx_to_csv-data/"
  val extensions = Seq(XLSX_EXTENSION, Constants.XLS_EXTENSION)
  val sparkConfig: SparkConfig =
    SparkConfig("handler-app-test", "local[*]", WARN)

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
        val path = Paths.get(file.getPath)
        val directory = new Directory(file)
        directory.deleteRecursively()
        Files.deleteIfExists(path)
      })
  }

  "CsvSink.saveCsvAsParquet" should "save a parquet as a csv file" in {
    import spark.implicits._

    CsvSink
      .saveParquetAsCsv(folderParquetToCsvData + "input/mock-data-2",
                        folderParquetToCsvData + "output/mock-data-2",
                        SaveMode.Overwrite,
                        sparkConfig)
    val actual =
      CsvSink.readParquet(folderParquetToCsvData + "output/mock-data-2",
                          sparkConfig)

    val expected = List(
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

    assertSmallDatasetEquality(actual.right.get.orderBy("id"),
                               expected,
                               ignoreNullable = true)
  }

  "CsvSink.saveJsonAsCsv" should "save a json as a csv file" in {
    import spark.implicits._

    CsvSink
      .saveJsonAsCsv(folderJsonToCsvData + "input/mock-data-2",
                     folderJsonToCsvData + "output/mock-data-2",
                     SaveMode.Overwrite,
                     sparkConfig)
    val actual =
      CsvSink.readParquet(folderJsonToCsvData + "output/mock-data-2",
                          sparkConfig)

    val expected = List(
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

    assertSmallDatasetEquality(actual.right.get
                                 .orderBy("id")
                                 .select("id",
                                         "first_name",
                                         "last_name",
                                         "email",
                                         "gender",
                                         "ip_address"),
                               expected,
                               ignoreNullable = true)
  }

  "CsvSink.saveAvroAsCsv" should "save a avro as a csv file" in {
    import spark.implicits._

    CsvSink
      .saveAvroAsCsv(folderAvroToCsvData + "input/mock-data-2",
                     folderAvroToCsvData + "output/mock-data-2",
                     SaveMode.Overwrite,
                     sparkConfig)
    val actual =
      CsvSink.readParquet(folderAvroToCsvData + "output/mock-data-2",
                          sparkConfig)

    val expected = List(
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

    assertSmallDatasetEquality(actual.right.get
                                 .orderBy("id")
                                 .select("id",
                                         "first_name",
                                         "last_name",
                                         "email",
                                         "gender",
                                         "ip_address"),
                               expected,
                               ignoreNullable = true)
  }

  "CsvSink.convertXlsxFileToCsvFile" should "convert xlsx sheet to a csv file" in {

    CsvSink
      .convertXlsxSheetToCsvFile("something",
                                 MockSheetCreator.createXlsxSheet("new-sheet"),
                                 folderXlsxCsvData + "output/")
      .map(path => path.toUri.getPath)
      .map(str => str.split(File.separatorChar).last)
    val d = new File(folderXlsxCsvData + "output/something/")
    d.listFiles
      .map(file => file.getName)
      .toList should contain theSameElementsAs List(
      "new-sheet.csv"
    )
  }

  "CsvSink.convertXlsxFileToCsvFiles" should "convert xlsx file to multiple csv files" in {

    val inputStream =
      new FileInputStream(folderXlsxCsvData + "input/mock-xlsx-data-1.xlsx")
    CsvSink.convertXlsxFileToCsvFiles("mock-xlsx-data-1",
                                      inputStream,
                                      folderXlsxCsvData + "output/")
    val d = new File(folderXlsxCsvData + "output/mock-xlsx-data-1")
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

    CsvSink.apply(folderXlsxCsvData + "input/",
                  folderXlsxCsvData + "output/",
                  XlsxCsv,
                  SaveMode.Overwrite,
                  sparkConfig)
    val list1 = List(
      "data1.csv",
      "data2.csv",
      "data3.csv",
      "data4.csv",
      "data5.csv",
    )
    val list2 = List(
      "data6.csv",
      "data7.csv",
      "data8.csv",
      "data9.csv",
      "data10.csv",
    )

    val dir1 = new File(folderXlsxCsvData + "output/mock-xlsx-data-1")
    dir1.listFiles
      .map(file => file.getName)
      .toList should contain allElementsOf list1

    val dir2 = new File(folderXlsxCsvData + "output/mock-xlsx-data-2")
    dir2.listFiles
      .map(file => file.getName)
      .toList should contain allElementsOf list2

    val dir3 =
      new File(folderXlsxCsvData + "output/folder1/folder3/mock-xlsx-data-31")
    dir3.listFiles
      .map(file => file.getName)
      .toList should contain allElementsOf list1

    val dir4 =
      new File(folderXlsxCsvData + "output/folder1/folder3/mock-xlsx-data-32")
    dir4.listFiles
      .map(file => file.getName)
      .toList should contain allElementsOf list2

    val dir5 = new File(folderXlsxCsvData + "output/folder1/mock-xlsx-data-11")
    dir5.listFiles
      .map(file => file.getName)
      .toList should contain allElementsOf list1

    val dir6 = new File(folderXlsxCsvData + "output/folder1/mock-xlsx-data-12")
    dir6.listFiles
      .map(file => file.getName)
      .toList should contain allElementsOf list2

    val dir7 = new File(folderXlsxCsvData + "output/folder2/mock-xlsx-data-21")
    dir7.listFiles
      .map(file => file.getName)
      .toList should contain allElementsOf list1

    val dir8 = new File(folderXlsxCsvData + "output/folder2/mock-xlsx-data-22")
    dir8.listFiles
      .map(file => file.getName)
      .toList should contain allElementsOf list2
  }

  "CsvSink.createPathRecursively" should "create a directory and its subdirectories" in {
    val str = CsvSink.createPathRecursively(
      "src/test/resources/xlsx_to_csv-data/output/sub1/sub2/sub3")
    Files.exists(Paths.get(str)) shouldBe true
    beforeEach() // delete folderXlsxCsvData
  }
}
