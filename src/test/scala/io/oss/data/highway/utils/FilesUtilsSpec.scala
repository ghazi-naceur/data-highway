package io.oss.data.highway.utils

import java.io.File

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FilesUtilsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  val folder = "src/test/resources/xlsx_to_csv-data/"
  val extensions = Seq(Constants.XLSX_EXTENSION, Constants.XLS_EXTENSION)

  "FilesUtils.getFilesFromPath" should "list all files in folder" in {

    val files = FilesUtils
      .getFilesFromPath(folder + "input/folder2", extensions)
      .map(list => list.map(str => str.split(File.separatorChar).last))
    files.map(fs => {
      fs should contain theSameElementsAs List(
        "mock-xlsx-data-21.xlsx",
        "mock-xlsx-data-22.xlsx"
      )
    })
  }

  "FilesUtils.getFilesFromPath" should "list a file" in {

    val files = FilesUtils
      .getFilesFromPath(folder + "input/mock-xlsx-data-1.xlsx", extensions)
      .map(list => list.map(str => str.split(File.separatorChar).last))
    files.map(fs => {
      fs should contain theSameElementsAs List("mock-xlsx-data-1.xlsx")
    })
  }

  "FilesUtils.getFilesFromPath" should "not list files" in {

    val files = FilesUtils
      .getFilesFromPath(folder + "input/empty.doc", extensions)
      .map(list => list.map(str => str.split(File.separatorChar).last))
    files.map(fs => {
      fs should contain theSameElementsAs Nil
    })
  }

  "FilesUtils.listFilesRecursively" should "list the files under a provided path" in {
    val list = FilesUtils
      .listFilesRecursively(
        new File("src/test/resources/xlsx_to_csv-data/input/folder2"),
        extensions)
      .toList
    val actual: List[String] =
      list.map(file => FilesUtils.reversePathSeparator(file.toString))
    val expected = List(
      "src/test/resources/xlsx_to_csv-data/input/folder2/mock-xlsx-data-21.xlsx",
      "src/test/resources/xlsx_to_csv-data/input/folder2/mock-xlsx-data-22.xlsx")

    actual should contain theSameElementsAs expected
  }

  "FilesUtils.filterByExtension" should "valid file's extension againt the provided ones : 1st test" in {

    val bool = FilesUtils.filterByExtension("fkfj/ffkfj/fkfjkf/file.txt",
                                            Seq("txt", "lks"))
    bool shouldBe true
  }

  "FilesUtils.filterByExtension" should "valid file's extension againt the provided ones : 2nd test" in {

    val bool = FilesUtils.filterByExtension("fkfj/ffkfj/fkfjkf/file.txt",
                                            Seq("txtt", "lks"))
    bool shouldBe false
  }

}
