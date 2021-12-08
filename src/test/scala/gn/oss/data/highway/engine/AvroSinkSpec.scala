package gn.oss.data.highway.engine

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import gn.oss.data.highway.engine.converter.FileConverter
import gn.oss.data.highway.helper.TestHelper
import gn.oss.data.highway.models.{AVRO, CSV, JSON, ORC, PARQUET, XLSX, XML}
import gn.oss.data.highway.utils.DataFrameUtils
import org.apache.spark.sql.SaveMode
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AvroSinkSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach with DatasetComparer with TestHelper {

  override def beforeEach(): Unit = {
    deleteFolderWithItsContent(avroFolder + "output")
  }

  "BasicSink.convert" should "convert parquet to avro" in {
    FileConverter
      .convert(
        PARQUET(None),
        parquetFolder + "input/mock-data-2",
        AVRO,
        avroFolder + "output/mock-data-2",
        SaveMode.Overwrite
      )
    val actual = DataFrameUtils
      .loadDataFrame(AVRO, avroFolder + "output/mock-data-2")
      .right
      .get
      .orderBy("id")
      .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convert json to avro" in {
    FileConverter
      .convert(JSON, jsonFolder + "input/mock-data-2", AVRO, avroFolder + "output/mock-data-2", SaveMode.Overwrite)
    val actual =
      DataFrameUtils
        .loadDataFrame(AVRO, avroFolder + "output/mock-data-2")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convert csv to avro" in {
    FileConverter
      .convert(
        CSV(inferSchema = true, header = true, ";"),
        csvFolder + "input/mock-data-2",
        AVRO,
        avroFolder + "output/mock-data-2",
        SaveMode.Overwrite
      )
    val actual =
      DataFrameUtils
        .loadDataFrame(AVRO, avroFolder + "output/mock-data-2")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convert orc to avro" in {
    FileConverter
      .convert(ORC(None), orcFolder + "input/mock-data-2", AVRO, avroFolder + "output/mock-data-2", SaveMode.Overwrite)
    val actual =
      DataFrameUtils
        .loadDataFrame(AVRO, avroFolder + "output/mock-data-2")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convert xml to avro" in {
    FileConverter
      .convert(
        XML("persons", "person"),
        xmlFolder + "input/mock-data-2",
        AVRO,
        avroFolder + "output/mock-data-2",
        SaveMode.Overwrite
      )
    val actual =
      DataFrameUtils
        .loadDataFrame(AVRO, avroFolder + "output/mock-data-2")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convert xlsx to avro" in {
    FileConverter.convert(
      XLSX,
      xlsxFolder + "input/folder1/mock-xlsx-data-13.xlsx",
      AVRO,
      avroFolder + "output/folder1/mock-xlsx-data-13",
      SaveMode.Overwrite
    )
    val actual =
      DataFrameUtils
        .loadDataFrame(AVRO, avroFolder + "output/folder1/mock-xlsx-data-13")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected2, ignoreNullable = true)
  }
}
