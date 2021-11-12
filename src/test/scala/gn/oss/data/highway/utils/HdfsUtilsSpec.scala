package gn.oss.data.highway.utils

import gn.oss.data.highway.configs.HdfsUtils
import gn.oss.data.highway.helper.TestHelper
import org.scalactic.source.Position
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HdfsUtilsSpec extends AnyFlatSpec with Matchers with BeforeAndAfter with TestHelper with HdfsUtils {

  override protected def after(fun: => Any)(implicit pos: Position): Unit = {
    deleteFolderWithItsContent(hdfsEntity.hdfsUri + "/tmp/data-highway")
  }

  "HdfsUtils.save" should "save a file" in {
    val time = System.currentTimeMillis().toString
    HdfsUtils
      .save(hdfsEntity.fs, hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/files/file.txt", "some content")
    val files =
      HdfsUtils.listFiles(hdfsEntity.fs, hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/files")
    files.head shouldBe hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/files/file.txt"
  }

  "HdfsUtils.save" should "throw an exception" in {
    val result = HdfsUtils
      .save(hdfsEntity.fs, "", "some content")
    result.left.get shouldBe a[Throwable]
  }

  "HdfsUtils.mkdir" should "create a folder" in {
    val time = System.currentTimeMillis().toString
    val result = HdfsUtils
      .mkdir(hdfsEntity.fs, hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/files")
    result.right.get shouldBe true
  }

  "HdfsUtils.mkdir" should "throw an exception" in {
    val result = HdfsUtils
      .mkdir(null, "some content")
    result.left.get shouldBe a[Throwable]
  }

  "HdfsUtils.move" should "move files" in {
    val time = System.currentTimeMillis().toString
    HdfsUtils
      .save(
        hdfsEntity.fs,
        hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset/file.json",
        "{\"some-key\":\"some value\"}"
      )
    HdfsUtils
      .save(
        hdfsEntity.fs,
        hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset/file2.json",
        "{\"some-key\":\"some value\"}"
      )
    HdfsUtils
      .mkdir(hdfsEntity.fs, hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/processed")
    val result = HdfsUtils
      .move(hdfsEntity.fs, s"/tmp/data-highway/$time/input/dataset", s"/tmp/data-highway/$time/processed")
    val files = HdfsUtils
      .listFiles(hdfsEntity.fs, hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/processed/dataset")
    result.right.get shouldBe true
    files should contain theSameElementsAs List(
      hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/processed/dataset/file.json",
      hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/processed/dataset/file2.json"
    )
  }

  "HdfsUtils.move" should "throw an exception" in {
    val result = HdfsUtils.move(null, "", "")
    result.left.get shouldBe a[Throwable]
  }

  "HdfsUtils.cleanup" should "cleanup a folder" in {
    val time = System.currentTimeMillis().toString
    HdfsUtils
      .save(
        hdfsEntity.fs,
        hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset/file.json",
        "{\"some-key\":\"some value\"}"
      )
    HdfsUtils
      .save(
        hdfsEntity.fs,
        hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset/file2.json",
        "{\"some-key\":\"some value\"}"
      )
    val result =
      HdfsUtils.cleanup(hdfsEntity.fs, hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input")
    val files = HdfsUtils
      .listFiles(hdfsEntity.fs, hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input")
    result.right.get shouldBe true
    files shouldBe List()
  }

  "HdfsUtils.cleanup" should "throw an exception" in {
    val result = HdfsUtils.cleanup(null, "")
    result.left.get shouldBe a[Throwable]
  }

  "HdfsUtils.listFolders" should "list sub-folders inside parent folder" in {
    val time = System.currentTimeMillis().toString
    HdfsUtils
      .save(
        hdfsEntity.fs,
        hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset/file.json",
        "{\"some-key\":\"some value\"}"
      )
    HdfsUtils.mkdir(hdfsEntity.fs, hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/processed")
    HdfsUtils.mkdir(hdfsEntity.fs, hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/output")
    HdfsUtils.mkdir(hdfsEntity.fs, hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/pending")
    HdfsUtils.mkdir(hdfsEntity.fs, hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/rejected")

    val result = HdfsUtils
      .listFolders(hdfsEntity.fs, hdfsEntity.hdfsUri + s"/tmp/data-highway/$time")
    result.right.get should contain theSameElementsAs List(
      hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input",
      hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/processed",
      hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/output",
      hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/pending",
      hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/rejected"
    )
  }

  "HdfsUtils.listFolders" should "throw an exception" in {
    val result = HdfsUtils.listFolders(null, "")
    result.left.get shouldBe a[Throwable]
  }

  "HdfsUtils.listFiles" should "list files inside a folder" in {
    val time = System.currentTimeMillis().toString
    HdfsUtils
      .save(
        hdfsEntity.fs,
        hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset/file.json",
        "{\"some-key\":\"some value\"}"
      )
    HdfsUtils
      .save(
        hdfsEntity.fs,
        hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset/file2.json",
        "{\"some-key\":\"some value\"}"
      )
    val result = HdfsUtils
      .listFiles(hdfsEntity.fs, hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset")
    result should contain theSameElementsAs List(
      hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset/file.json",
      hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset/file2.json"
    )
  }

  "HdfsUtils.movePathContent" should "move files from a path to another" in {
    val time = System.currentTimeMillis().toString
    HdfsUtils
      .save(
        hdfsEntity.fs,
        hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset/file.json",
        "{\"some-key\":\"some value\"}"
      )
    HdfsUtils
      .save(
        hdfsEntity.fs,
        hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset/file2.json",
        "{\"some-key\":\"some value\"}"
      )

    val result = HdfsUtils
      .movePathContent(
        hdfsEntity.fs,
        hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset",
        hdfsEntity.hdfsUri + s"/tmp/data-highway/$time"
      )
    result.right.get should contain theSameElementsAs List(
      hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/processed/dataset/file.json",
      hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/processed/dataset/file2.json"
    )

    val files = HdfsUtils
      .listFiles(hdfsEntity.fs, hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/processed/dataset")
    files should contain theSameElementsAs List(
      hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/processed/dataset/file.json",
      hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/processed/dataset/file2.json"
    )
  }

  "HdfsUtils.movePathContent" should "throw an exception" in {
    val result = HdfsUtils.movePathContent(null, "", "")
    result.left.get shouldBe a[Throwable]
  }

  "HdfsUtils.getPathWithoutUriPrefix" should "get the path without the URI prefix 'hdfs://host:port'" in {
    val result = HdfsUtils.getPathWithoutUriPrefix("hdfs://localhost:36537/tmp/data-highway-564578894565/input/dataset")
    result shouldBe "/tmp/data-highway-564578894565/input/dataset"
  }

  "HdfsUtils.filterNonEmptyFolders" should "filter non-empty folders" in {
    val time = System.currentTimeMillis().toString
    HdfsUtils
      .save(
        hdfsEntity.fs,
        hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset1/file.json",
        "{\"some-key\":\"some value\"}"
      )

    HdfsUtils
      .save(
        hdfsEntity.fs,
        hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset2/file.json",
        "{\"some-key\":\"some value\"}"
      )
    HdfsUtils.mkdir(hdfsEntity.fs, hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset3")
    HdfsUtils.mkdir(hdfsEntity.fs, hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset4")
    HdfsUtils.mkdir(hdfsEntity.fs, hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset5")

    val folders = List(
      hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset1",
      hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset2",
      hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset3",
      hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset4",
      hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset5"
    )
    val result = HdfsUtils
      .filterNonEmptyFolders(hdfsEntity.fs, folders)
    result.right.get should contain theSameElementsAs List(
      hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset1",
      hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset2"
    )
  }

  "HdfsUtils.filterNonEmptyFolders" should "throw an exception" in {
    val result = HdfsUtils.filterNonEmptyFolders(null, List(""))
    result.left.get shouldBe a[Throwable]
  }

  "HdfsUtils.listFilesRecursively" should "list files recursively from a path" in {
    val time = System.currentTimeMillis().toString
    HdfsUtils
      .save(
        hdfsEntity.fs,
        hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset1/file.json",
        "{\"some-key\":\"some value\"}"
      )
    HdfsUtils
      .save(
        hdfsEntity.fs,
        hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset1/file2.json",
        "{\"some-key\":\"some value\"}"
      )
    HdfsUtils
      .save(
        hdfsEntity.fs,
        hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset2/file.json",
        "{\"some-key\":\"some value\"}"
      )
    HdfsUtils.mkdir(hdfsEntity.fs, hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset3")
    HdfsUtils.mkdir(hdfsEntity.fs, hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset4")
    HdfsUtils.mkdir(hdfsEntity.fs, hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input/dataset5")

    val result = HdfsUtils
      .listFilesRecursively(hdfsEntity.fs, hdfsEntity.hdfsUri + s"/tmp/data-highway/$time/input")
    result should contain theSameElementsAs List(
      s"${hadoopConf.host}:${hadoopConf.port}" + s"/tmp/data-highway/$time/input/dataset1/file.json",
      s"${hadoopConf.host}:${hadoopConf.port}" + s"/tmp/data-highway/$time/input/dataset1/file2.json",
      s"${hadoopConf.host}:${hadoopConf.port}" + s"/tmp/data-highway/$time/input/dataset2/file.json"
    )
  }
}
