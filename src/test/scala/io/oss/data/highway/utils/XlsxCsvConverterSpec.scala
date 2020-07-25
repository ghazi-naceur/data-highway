package io.oss.data.highway.utils

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Paths}

import io.oss.data.highway.utils.Constants.XLSX_EXTENSION
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.reflect.io.Directory


class XlsxCsvConverterSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  val folder = "src/test/resources/xlsx_to_csv-data/"
  val extensions = Seq(XLSX_EXTENSION, Constants.XLS_EXTENSION)

  override def beforeEach(): Unit = {
    new File(folder + "output").listFiles.toList.filterNot(_.getName.endsWith(".gitkeep")).foreach(file => {
      val path = Paths.get(file.getPath)
      val directory = new Directory(file)
      directory.deleteRecursively()
      Files.deleteIfExists(path)
    })
  }

  "XlsxCsvConverter.convertXlsxFileToCsvFile" should "convert xlsx sheet to a csv file" in {

    XlsxCsvConverter.convertXlsxSheetToCsvFile("something", MockSheetCreator.createXlsxSheet("new-sheet"), folder + "output/")
      .map(path => path.toUri.getPath).map(str => str.split(File.separatorChar).last)
    val d = new File(folder + "output/something/")
    d.listFiles.map(file => file.getName).toList should contain theSameElementsAs List(
      "new-sheet.csv"
    )
  }

  "XlsxCsvConverter.convertXlsxFileToCsvFiles" should "convert xlsx file to multiple csv files" in {

    val inputStream = new FileInputStream(folder + "input/mock-xlsx-data-1.xlsx")
    XlsxCsvConverter.convertXlsxFileToCsvFiles("mock-xlsx-data-1", inputStream, folder + "output/")
    val d = new File(folder + "output/mock-xlsx-data-1")
    d.listFiles.map(file => file.getName).toList should contain allElementsOf List(
      "data1.csv",
      "data2.csv",
      "data3.csv",
      "data4.csv",
      "data5.csv"
    )
  }

  "XlsxCsvConverter.apply" should "convert xlsx files to multiple csv files" in {

    XlsxCsvConverter.apply(folder + "input/", folder + "output/", Seq(XLSX_EXTENSION, XLSX_EXTENSION))
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

    val dir1 = new File(folder + "output/mock-xlsx-data-1")
    dir1.listFiles.map(file => file.getName).toList should contain allElementsOf list1

    val dir2 = new File(folder + "output/mock-xlsx-data-2")
    dir2.listFiles.map(file => file.getName).toList should contain allElementsOf list2

    val dir3 = new File(folder + "output/folder1/folder3/mock-xlsx-data-31")
    dir3.listFiles.map(file => file.getName).toList should contain allElementsOf list1

    val dir4 = new File(folder + "output/folder1/folder3/mock-xlsx-data-32")
    dir4.listFiles.map(file => file.getName).toList should contain allElementsOf list2

    val dir5 = new File(folder + "output/folder1/mock-xlsx-data-11")
    dir5.listFiles.map(file => file.getName).toList should contain allElementsOf list1

    val dir6 = new File(folder + "output/folder1/mock-xlsx-data-12")
    dir6.listFiles.map(file => file.getName).toList should contain allElementsOf list2

    val dir7 = new File(folder + "output/folder2/mock-xlsx-data-21")
    dir7.listFiles.map(file => file.getName).toList should contain allElementsOf list1

    val dir8 = new File(folder + "output/folder2/mock-xlsx-data-22")
    dir8.listFiles.map(file => file.getName).toList should contain allElementsOf list2
  }

  "XlsxCsvConverter.createPathRecursively" should "create a directory and its subdirectories" in {
    val str = XlsxCsvConverter.createPathRecursively("src/test/resources/xlsx_to_csv-data/output/sub1/sub2/sub3")
    Files.exists(Paths.get(str)) shouldBe true
    beforeEach() // delete folder
  }
}
