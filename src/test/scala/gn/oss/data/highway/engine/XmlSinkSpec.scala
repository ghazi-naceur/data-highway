package gn.oss.data.highway.engine

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import gn.oss.data.highway.engine.sinks.BasicSink
import gn.oss.data.highway.helper.TestHelper
import gn.oss.data.highway.models.{AVRO, CSV, JSON, ORC, PARQUET, XLSX, XML}
import gn.oss.data.highway.utils.{DataFrameUtils, FilesUtils}
import org.apache.spark.sql.SaveMode
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class XmlSinkSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach with DatasetComparer with TestHelper {

  override def beforeEach(): Unit = {
    deleteFolderWithItsContent(xmlFolder + "output")
  }

  "BasicSink.convert" should "convert csv to xml" in {
    BasicSink.convert(
      CSV(inferSchema = true, header = true, ";"),
      csvFolder + "input/mock-data-2",
      XML("persons", "person"),
      xmlFolder + "output/mock-data-2",
      SaveMode.Overwrite
    )
    val filename =
      FilesUtils
        .listFiles(List(xmlFolder + "output/mock-data-2"))
        .right
        .get
        .filterNot(_.getName.startsWith("."))
        .head
        .getName
    val actual =
      DataFrameUtils
        .loadDataFrame(XML("persons", "person"), xmlFolder + s"output/mock-data-2/$filename")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convert json to xml" in {
    BasicSink
      .convert(
        JSON,
        jsonFolder + "input/mock-data-2",
        XML("persons", "person"),
        xmlFolder + "output/mock-data-2",
        SaveMode.Overwrite
      )
    val filename =
      FilesUtils
        .listFiles(List(xmlFolder + "output/mock-data-2"))
        .right
        .get
        .filterNot(_.getName.startsWith("."))
        .head
        .getName
    val actual =
      DataFrameUtils
        .loadDataFrame(XML("persons", "person"), xmlFolder + s"output/mock-data-2/$filename")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convert parquet to xml" in {
    BasicSink.convert(
      PARQUET(None),
      parquetFolder + "input/mock-data-2",
      XML("persons", "person"),
      xmlFolder + "output/mock-data-2",
      SaveMode.Overwrite
    )
    val filename =
      FilesUtils
        .listFiles(List(xmlFolder + "output/mock-data-2"))
        .right
        .get
        .filterNot(_.getName.startsWith("."))
        .head
        .getName
    val actual =
      DataFrameUtils
        .loadDataFrame(XML("persons", "person"), xmlFolder + s"output/mock-data-2/$filename")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convert orc to xml" in {
    BasicSink
      .convert(
        ORC(None),
        orcFolder + "input/mock-data-2",
        XML("persons", "person"),
        xmlFolder + "output/mock-data-2",
        SaveMode.Overwrite
      )
    val filename =
      FilesUtils
        .listFiles(List(xmlFolder + "output/mock-data-2"))
        .right
        .get
        .filterNot(_.getName.startsWith("."))
        .head
        .getName
    val actual =
      DataFrameUtils
        .loadDataFrame(XML("persons", "person"), xmlFolder + s"output/mock-data-2/$filename")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convert avro to xml" in {
    BasicSink
      .convert(
        AVRO,
        avroFolder + "input/mock-data-2",
        XML("persons", "person"),
        xmlFolder + "output/mock-data-2",
        SaveMode.Overwrite
      )
    val filename =
      FilesUtils
        .listFiles(List(xmlFolder + "output/mock-data-2"))
        .right
        .get
        .filterNot(_.getName.startsWith("."))
        .head
        .getName
    val actual =
      DataFrameUtils
        .loadDataFrame(XML("persons", "person"), xmlFolder + s"output/mock-data-2/$filename")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convert xlsx to parquet" in {
    BasicSink.convert(
      XLSX,
      xlsxFolder + "input/folder1/mock-xlsx-data-13.xlsx",
      XML("persons", "person"),
      xmlFolder + "output/folder1/mock-xlsx-data-13",
      SaveMode.Overwrite
    )
    val actual =
      DataFrameUtils
        .loadDataFrame(XML("persons", "person"), xmlFolder + "output/folder1/mock-xlsx-data-13")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected2, ignoreNullable = true)
  }
}
