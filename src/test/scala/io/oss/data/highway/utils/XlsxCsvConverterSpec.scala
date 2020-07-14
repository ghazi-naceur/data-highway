package io.oss.data.highway.utils

import java.io.{File, FileInputStream}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers



class XlsxCsvConverterSpec extends AnyFlatSpec with Matchers {

  val folder = "src/test/resources/xlsx-data/"

  "XlsxCsvConverter.processBddCsvConversion" should "launch the sheet-csv conversion process" in {

    val inputStream = new FileInputStream(folder + "input/mock-xlsx-data-1.xlsx")
    XlsxCsvConverter.convertXlsxFileToCsvFiles(inputStream, folder + "output/")
    val d = new File(folder + "output/")
    d.listFiles.map(file => file.getName).toList.filterNot(_ == ".gitkeep") should contain allElementsOf List(
      "data1.csv",
      "data2.csv",
      "data3.csv",
      "data4.csv",
      "data5.csv"
    )
  }
}
