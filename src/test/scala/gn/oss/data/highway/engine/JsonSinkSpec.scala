package gn.oss.data.highway.engine

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import gn.oss.data.highway.models.{AVRO, CSV, JSON, PARQUET, XLSX}
import gn.oss.data.highway.utils.{DataFrameUtils, TestHelper}
import org.apache.spark.sql.{SaveMode, functions}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JsonSinkSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterEach
    with DatasetComparer
    with TestHelper {

  override def beforeEach(): Unit = {
    deleteFolderWithItsContent(jsonFolder + "output")
  }

  "BasicSink.convert" should "convert parquet to json" in {
    BasicSink.convert(
      PARQUET,
      parquetFolder + "input/mock-data-2",
      JSON,
      jsonFolder + "output/mock-data-2",
      SaveMode.Overwrite
    )
    val actual =
      DataFrameUtils
        .loadDataFrame(JSON, jsonFolder + "output/mock-data-2")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")
    val map = actual
      .toLocalIterator()
      .next()
      .getValuesMap(actual.toLocalIterator().next().schema.fieldNames)
    new scala.util.parsing.json.JSONObject(
      map
    )
    actual.toJSON

//    val fieldNames = actual.head().schema.fieldNames
//    actual.foreach(row => {
//      val map1 = row.getValuesMap(fieldNames)
//      println(toJson(map1))
//    })

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convertn avro to json" in {
    BasicSink.convert(
      AVRO,
      avroFolder + "input/mock-data-2",
      JSON,
      jsonFolder + "output/mock-data-2",
      SaveMode.Overwrite
    )
    val actual =
      DataFrameUtils
        .loadDataFrame(JSON, jsonFolder + "output/mock-data-2")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convert csv to json" in {
    BasicSink.convert(
      CSV,
      csvFolder + "input/mock-data-2",
      JSON,
      jsonFolder + "output/mock-data-2",
      SaveMode.Overwrite
    )
    val actual =
      DataFrameUtils
        .loadDataFrame(JSON, jsonFolder + "output/mock-data-2")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convert xlsx to json" in {
    BasicSink.convert(
      XLSX,
      xlsxFolder + "input/folder1/mock-xlsx-data-13.xlsx",
      JSON,
      jsonFolder + "output/folder1/mock-xlsx-data-13",
      SaveMode.Overwrite
    )
    val actual =
      DataFrameUtils
        .loadDataFrame(JSON, jsonFolder + "output/folder1/mock-xlsx-data-13")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected2, ignoreNullable = true)
  }
}