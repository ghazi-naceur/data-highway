package gn.oss.data.highway.engine

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import gn.oss.data.highway.models.{AVRO, CSV, JSON, ORC, PARQUET, XLSX}
import gn.oss.data.highway.utils.{DataFrameUtils, FilesUtils, TestHelper}
import org.apache.spark.sql.SaveMode
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class XlsxSinkSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterEach
    with DatasetComparer
    with TestHelper {

  override def beforeEach(): Unit = {
    deleteFolderWithItsContent(xlsxFolder + "output")
  }

  "BasicSink.convert" should "convert csv to xlsx" in {
    BasicSink.convert(
      CSV,
      csvFolder + "input/mock-data-2",
      XLSX,
      xlsxFolder + "output/mock-data-2",
      SaveMode.Overwrite
    )
    val filename =
      FilesUtils
        .listFiles(List(xlsxFolder + "output/mock-data-2"))
        .right
        .get
        .filterNot(_.getName.startsWith("."))
        .head
        .getName
    val actual =
      DataFrameUtils
        .loadDataFrame(XLSX, xlsxFolder + s"output/mock-data-2/$filename")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convert json to xlsx" in {
    BasicSink.convert(
      JSON,
      jsonFolder + "input/mock-data-2",
      XLSX,
      xlsxFolder + "output/mock-data-2",
      SaveMode.Overwrite
    )
    val filename =
      FilesUtils
        .listFiles(List(xlsxFolder + "output/mock-data-2"))
        .right
        .get
        .filterNot(_.getName.startsWith("."))
        .head
        .getName
    val actual =
      DataFrameUtils
        .loadDataFrame(XLSX, xlsxFolder + s"output/mock-data-2/$filename")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convert parquet to xlsx" in {
    BasicSink.convert(
      PARQUET(None),
      parquetFolder + "input/mock-data-2",
      XLSX,
      xlsxFolder + "output/mock-data-2",
      SaveMode.Overwrite
    )
    val filename =
      FilesUtils
        .listFiles(List(xlsxFolder + "output/mock-data-2"))
        .right
        .get
        .filterNot(_.getName.startsWith("."))
        .head
        .getName
    val actual =
      DataFrameUtils
        .loadDataFrame(XLSX, xlsxFolder + s"output/mock-data-2/$filename")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convert orc to xlsx" in {
    BasicSink
      .convert(
        ORC(None),
        orcFolder + "input/mock-data-2",
        XLSX,
        xlsxFolder + "output/mock-data-2",
        SaveMode.Overwrite
      )
    val filename =
      FilesUtils
        .listFiles(List(xlsxFolder + "output/mock-data-2"))
        .right
        .get
        .filterNot(_.getName.startsWith("."))
        .head
        .getName
    val actual =
      DataFrameUtils
        .loadDataFrame(XLSX, xlsxFolder + s"output/mock-data-2/$filename")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convert avro to xlsx" in {
    BasicSink.convert(
      AVRO,
      avroFolder + "input/mock-data-2",
      XLSX,
      xlsxFolder + "output/mock-data-2",
      SaveMode.Overwrite
    )
    val filename =
      FilesUtils
        .listFiles(List(xlsxFolder + "output/mock-data-2"))
        .right
        .get
        .filterNot(_.getName.startsWith("."))
        .head
        .getName
    val actual =
      DataFrameUtils
        .loadDataFrame(XLSX, xlsxFolder + s"output/mock-data-2/$filename")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }
}
