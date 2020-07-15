package io.oss.data.highway.utils

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Paths}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class XlsxCsvConverterSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  val folder = "src/test/resources/xlsx-data/"

  override def beforeEach(): Unit = {
    new File(folder + "output").listFiles.toList.filter(_.getName != ".gitkeep").foreach(file => {
      val path = Paths.get(file.getPath)
      Files.delete(path)
    })
  }

  "XlsxCsvConverter.convertXlsxFileToCsvFile" should "convert xlsx sheet to a csv file" in {

    XlsxCsvConverter.convertXlsxSheetToCsvFile(MockSheetCreator.createXlsxSheet("new-sheet"), folder + "output/")
      .map(path => path.toUri.getPath).map(str => str.split("/").last)
    val d = new File(folder + "output/")
    d.listFiles.map(file => file.getName).filterNot(_ == ".gitkeep").toList should contain theSameElementsAs List(
      "new-sheet.csv"
    )
  }

  "XlsxCsvConverter.getFilesFromPath" should "list all files in folder" in {

    val files = XlsxCsvConverter.getFilesFromPath(folder + "input/").map(list => list.map(str => str.split("\\\\").last))
    files.map(fs => {
      fs should contain theSameElementsAs List(
        "mock-xlsx-data-1.xlsx",
        "mock-xlsx-data-2.xlsx"
      )
    })
  }

  "XlsxCsvConverter.convertXlsxFileToCsvFiles" should "convert xlsx file to multiple csv files" in {

    val inputStream = new FileInputStream(folder + "input/mock-xlsx-data-1.xlsx")
    XlsxCsvConverter.convertXlsxFileToCsvFiles(inputStream, folder + "output/")
    val d = new File(folder + "output/")
    d.listFiles.map(file => file.getName).toList should contain allElementsOf List(
      "data1.csv",
      "data2.csv",
      "data3.csv",
      "data4.csv",
      "data5.csv"
    )
  }

  "XlsxCsvConverter.apply" should "convert xlsx files to multiple csv files" in {

    XlsxCsvConverter.apply(folder + "input/", folder + "output/")
    val d = new File(folder + "output/")
    d.listFiles.map(file => file.getName).toList should contain allElementsOf List(
      "data1.csv",
      "data2.csv",
      "data3.csv",
      "data4.csv",
      "data5.csv",
      "data6.csv",
      "data7.csv",
      "data8.csv",
      "data9.csv",
      "data10.csv",
    )
  }

}
