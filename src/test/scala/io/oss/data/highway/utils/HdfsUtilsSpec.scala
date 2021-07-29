package io.oss.data.highway.utils

import io.oss.data.highway.models.DataHighwayError.HdfsError
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HdfsUtilsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach with FSUtils {

  "HdfsUtils.save" should "save a file" in {
    val time = System.currentTimeMillis().toString
    HdfsUtils
      .save(
        hdfsEntity.fs,
        hdfsEntity.hdfsUri + s"/tmp/data-highway-$time/files/file.txt",
        "some content"
      )
    val files = HdfsUtils.listFiles(hdfsEntity.fs, hdfsEntity.hdfsUri + "/tmp/data-highway/files")
    files.head shouldBe hdfsEntity.hdfsUri + "/tmp/data-highway/files/file.txt"
  }

  "HdfsUtils.save" should "throw an exception" in {
    val result = HdfsUtils
      .save(hdfsEntity.fs, "", "some content")
    result.left.get shouldBe a[HdfsError]
  }

  "HdfsUtils.mkdir" should "create a folder" in {
    val time = System.currentTimeMillis().toString
    val result = HdfsUtils
      .mkdir(hdfsEntity.fs, hdfsEntity.hdfsUri + s"/tmp/data-highway-$time/files")
    result.right.get shouldBe true
  }

  "HdfsUtils.mkdir" should "throw an exception" in {
    val result = HdfsUtils
      .mkdir(null, "some content")
    result.left.get shouldBe a[HdfsError]
  }

  "HdfsUtils.move" should "move files" in {
    val time = System.currentTimeMillis().toString
    HdfsUtils
      .save(
        hdfsEntity.fs,
        hdfsEntity.hdfsUri + s"/tmp/data-highway-$time/input/dataset/file.json",
        "{\"some-key\":\"some value\"}"
      )
    HdfsUtils
      .save(
        hdfsEntity.fs,
        hdfsEntity.hdfsUri + s"/tmp/data-highway-$time/input/dataset/file2.json",
        "{\"some-key\":\"some value\"}"
      )
    val result = HdfsUtils
      .move(
        hdfsEntity.fs,
        hdfsEntity.hdfsUri + s"/tmp/data-highway-$time/input/dataset",
        hdfsEntity.hdfsUri + s"/tmp/data-highway-$time/processed"
      )
    val files = HdfsUtils
      .listFiles(hdfsEntity.fs, hdfsEntity.hdfsUri + s"/tmp/data-highway-$time/processed/dataset")
    files.head shouldBe hdfsEntity.hdfsUri + s"/tmp/data-highway-$time/processed/dataset/file.json"
    files(1) shouldBe hdfsEntity.hdfsUri + s"/tmp/data-highway-$time/processed/dataset/file2.json"
  }
}
