package io.oss.data.highway.utils

import io.oss.data.highway.models.DataHighwayError.DataHighwayFileError

import java.io.{BufferedWriter, File, FileWriter}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class FilesUtilsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach with FSUtils {

  "FilesUtils.filterByExtension" should "valid file's extension againt the provided ones : 1st test" in {
    val bool = FilesUtils.filterByExtension("fkfj/ffkfj/fkfjkf/file.txt", Seq("txt", "lks"))
    bool shouldBe true
  }

  "FilesUtils.filterByExtension" should "valid file's extension againt the provided ones : 2nd test" in {
    val bool = FilesUtils.filterByExtension("fkfj/ffkfj/fkfjkf/file.txt", Seq("txtt", "lks"))
    bool shouldBe false
  }

  "FilesUtils.listNonEmptyFoldersRecursively" should "list folders recursively" in {
    val time    = System.currentTimeMillis().toString
    val srcPath = s"/tmp/data-highway/input-$time/dataset"
    Files.createDirectories(new File(srcPath).toPath)
    Files.createDirectories(new File(srcPath + "/a").toPath)
    Files.createFile(new File(srcPath + "/a/file.txt").toPath)
    Files.createDirectories(new File(srcPath + "/a/b").toPath)
    Files.createFile(new File(srcPath + "/a/b/file.txt").toPath)
    Files.createDirectories(new File(srcPath + "/c").toPath)
    Files.createFile(new File(srcPath + "/c/file.txt").toPath)
    Files.createDirectories(new File(srcPath + "/d").toPath)
    Files.createFile(new File(srcPath + "/d/file.txt").toPath)
    Files.createDirectories(new File(srcPath + "/d/e").toPath)
    Files.createFile(new File(srcPath + "/d/e/file.txt").toPath)
    val result = FilesUtils.listNonEmptyFoldersRecursively(srcPath)
    result.right.get should contain theSameElementsAs List(
      srcPath + "/a",
      srcPath + "/c",
      srcPath + "/d",
      srcPath + "/a/b",
      srcPath + "/d/e"
    )
  }

  "FilesUtils.listNonEmptyFoldersRecursively" should "throw an exception" in {
    val result = FilesUtils.listNonEmptyFoldersRecursively("")
    result.left.get shouldBe a[DataHighwayFileError]
  }

  "FilesUtils.reversePathSeparator" should "reverse path separator" in {
    val result = FilesUtils.reversePathSeparator("a\\b\\c")
    result shouldBe "a/b/c"
  }

  "FilesUtils.create" should "create a file" in {
    val time    = System.currentTimeMillis().toString
    val srcPath = s"/tmp/data-highway/input-$time/dataset"
    val result  = FilesUtils.createFile(srcPath, "file.txt", "some content")
    result.right.get shouldBe ()
    FilesUtils.getLines(srcPath + "/file.txt").toList.head shouldBe "some content"
  }

  "FilesUtils.create" should "throw an exception" in {
    val result = FilesUtils.createFile("", "file.txt", "some content")
    result.left.get shouldBe a[DataHighwayFileError]
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

  "FilesUtils.movePathContent" should "throw an exception" in {
    val time     = System.currentTimeMillis().toString
    val srcPath  = s"/tmp/data-highway/input-$time/dataset"
    val destPath = s"/tmp/data-highway/processed-$time"
    Files.createDirectories(new File(srcPath).toPath)
    Files.createFile(new File(srcPath + "/file.txt").toPath)
    val result = FilesUtils.movePathContent(srcPath + "/non-existent", destPath + "/non-existent")
    result.left.get shouldBe a[DataHighwayFileError]
  }

  "FilesUtils.cleanup" should "delete the path content" in {
    val time    = System.currentTimeMillis().toString
    val srcPath = s"/tmp/data-highway/input-$time/dataset"
    Files.createDirectories(new File(srcPath).toPath)
    Files.createFile(new File(srcPath + "/file.txt").toPath)
    FilesUtils.cleanup(srcPath)
    FilesUtils
      .listFiles(List(srcPath))
      .right
      .get shouldBe List()
  }

  "FilesUtils.getJsonLines" should "get lines from file" in {
    val time    = System.currentTimeMillis().toString
    val srcPath = s"/tmp/data-highway/input-$time/dataset"
    Files.createDirectories(new File(srcPath).toPath)
    val fstream = new FileWriter(srcPath + s"/file-$time.txt", true)
    val out     = new BufferedWriter(fstream)
    out.write("line1\nline2\nline3")
    out.close()
    val result = FilesUtils.getLines(srcPath + s"/file-$time.txt").toList
    result shouldBe List("line1", "line2", "line3")
  }

  "FilesUtils.filterNonEmptyFolders" should "return non empty folders" in {
    val time     = System.currentTimeMillis().toString
    val srcPath1 = s"/tmp/data-highway/input-$time/dataset1"
    val srcPath2 = s"/tmp/data-highway/input-$time/dataset2"
    val srcPath3 = s"/tmp/data-highway/input-$time/dataset3"
    Files.createDirectories(new File(srcPath1).toPath)
    Files.createFile(new File(srcPath1 + "/file.txt").toPath)
    Files.createDirectories(new File(srcPath2).toPath)
    Files.createDirectories(new File(srcPath3).toPath)
    Files.createFile(new File(srcPath3 + "/file.txt").toPath)

    val result = FilesUtils.filterNonEmptyFolders(List(srcPath1, srcPath2, srcPath3))
    result.right.get shouldBe List(srcPath1, srcPath3)
  }

  "FilesUtils.filterNonEmptyFolders" should "throw an exception" in {
    val time     = System.currentTimeMillis().toString
    val srcPath1 = s"/tmp/data-highway/input-$time/dataset1"
    val srcPath2 = s"/tmp/data-highway/input-$time/dataset2"
    val srcPath3 = s"/tmp/data-highway/input-$time/dataset3"

    val result = FilesUtils.filterNonEmptyFolders(List(srcPath1, srcPath2, srcPath3))
    result.left.get shouldBe a[DataHighwayFileError]
  }

  "FilesUtils.getFileNameAndParentFolderFromPath" should "get filename and its parent" in {
    val time    = System.currentTimeMillis().toString
    val srcPath = s"/tmp/data-highway/input-$time/dataset"
    Files.createDirectories(new File(srcPath).toPath)
    Files.createFile(new File(srcPath + "/file.txt").toPath)
    val result = FilesUtils.getFileNameAndParentFolderFromPath(srcPath + "/file.txt", "txt")
    result shouldBe "dataset/file"
  }
}
