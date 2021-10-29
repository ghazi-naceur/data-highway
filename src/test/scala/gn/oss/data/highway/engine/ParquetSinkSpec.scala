package gn.oss.data.highway.engine

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import gn.oss.data.highway.engine.sinks.BasicSink
import gn.oss.data.highway.helper.TestHelper
import gn.oss.data.highway.models.{AVRO, CSV, JSON, ORC, PARQUET, Snappy, XLSX}
import gn.oss.data.highway.utils.DataFrameUtils
import org.apache.spark.sql.SaveMode
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ParquetSinkSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterEach
    with DatasetComparer
    with TestHelper {

  override def beforeEach(): Unit = {
    deleteFolderWithItsContent(parquetFolder + "output")
  }

  "BasicSink.convert" should "convert csv to parquet" in {
    BasicSink.convert(
      CSV(inferSchema = true, header = true, ";"),
      csvFolder + "input/mock-data-2",
      PARQUET(Some(Snappy)),
      parquetFolder + "output/mock-data-2",
      SaveMode.Overwrite
    )
    val actual =
      DataFrameUtils
        .loadDataFrame(PARQUET(None), parquetFolder + "output/mock-data-2")
        .right
        .get
        .orderBy("id")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convert avro to parquet" in {
    BasicSink.convert(
      AVRO,
      avroFolder + "input/mock-data-2",
      PARQUET(Some(Snappy)),
      parquetFolder + "output/mock-data-2",
      SaveMode.Overwrite
    )
    val actual =
      DataFrameUtils
        .loadDataFrame(PARQUET(None), parquetFolder + "output/mock-data-2")
        .right
        .get
        .orderBy("id")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convert orc to parquet" in {
    BasicSink
      .convert(
        ORC(None),
        orcFolder + "input/mock-data-2",
        PARQUET(Some(Snappy)),
        parquetFolder + "output/mock-data-2",
        SaveMode.Overwrite
      )
    val actual =
      DataFrameUtils
        .loadDataFrame(PARQUET(None), parquetFolder + "output/mock-data-2")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convert json to parquet" in {
    BasicSink.convert(
      JSON,
      jsonFolder + "input/mock-data-2",
      PARQUET(Some(Snappy)),
      parquetFolder + "output/mock-data-2",
      SaveMode.Overwrite
    )
    val actual =
      DataFrameUtils
        .loadDataFrame(PARQUET(None), parquetFolder + "output/mock-data-2")
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
      PARQUET(Some(Snappy)),
      parquetFolder + "output/folder1/mock-xlsx-data-13",
      SaveMode.Overwrite
    )
    val actual =
      DataFrameUtils
        .loadDataFrame(PARQUET(None), parquetFolder + "output/folder1/mock-xlsx-data-13")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected2, ignoreNullable = true)
  }
}
