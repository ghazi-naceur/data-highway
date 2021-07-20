package io.oss.data.highway.utils

import java.io.File
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class FilesUtilsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach with FSUtils {

  val folder     = "src/test/resources/xlsx_to_csv-data/"
  val extensions = Seq("xlsx", "xls")

  "FilesUtils.getFilesFromPath" should "list all files in folderXlsxCsvData" in {
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

  "FilesUtils.filterByExtension" should "valid file's extension againt the provided ones : 1st test" in {
    val bool = FilesUtils.filterByExtension("fkfj/ffkfj/fkfjkf/file.txt", Seq("txt", "lks"))
    bool shouldBe true
  }

  "FilesUtils.filterByExtension" should "valid file's extension againt the provided ones : 2nd test" in {
    val bool = FilesUtils.filterByExtension("fkfj/ffkfj/fkfjkf/file.txt", Seq("txtt", "lks"))
    bool shouldBe false
  }

  "FilesUtils.movePathContent" should "move folder from source to destination" in {
    val time     = System.currentTimeMillis().toString
    val srcPath  = s"/tmp/data-highway/input-$time/dataset"
    val destPath = s"/tmp/data-highway/processed-$time"
    Files.createDirectories(new File(srcPath).toPath)
    Files.createFile(new File(srcPath + "/file.txt").toPath)
    val result = FilesUtils.movePathContent(srcPath, destPath)
    result.right.get.head shouldBe destPath
    FilesUtils
      .listFiles(List(destPath + "/dataset"))
      .right
      .get
      .head
      .getName shouldBe "file.txt"
  }

  "FilesUtils.movePathContent" should "move file from source to destination" in {
    val time     = System.currentTimeMillis().toString
    val srcPath  = s"/tmp/data-highway/input-$time/dataset"
    val destPath = s"/tmp/data-highway/processed-$time"
    Files.createDirectories(new File(srcPath).toPath)
    Files.createFile(new File(srcPath + "/file.txt").toPath)
    val result = FilesUtils.movePathContent(srcPath + "/file.txt", destPath)
    result.right.get.head shouldBe destPath
    FilesUtils
      .listFiles(List(destPath))
      .right
      .get
      .head
      .getName shouldBe "file.txt"
  }
}
