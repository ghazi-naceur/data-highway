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

class CsvSinkSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach with DatasetComparer with TestHelper {

  override def beforeEach(): Unit = {
    deleteFolderWithItsContent(csvFolder + "output")
  }

  "BasicSink.convert" should "convert parquet to csv" in {
    FileConverter.convert(
      PARQUET(None),
      parquetFolder + "input/mock-data-2",
      CSV(inferSchema = true, header = true, ";"),
      csvFolder + "output/mock-data-2",
      SaveMode.Overwrite
    )
    val actual =
      DataFrameUtils
        .loadDataFrame(CSV(inferSchema = true, header = true, ";"), csvFolder + "output/mock-data-2")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convert json to csv" in {
    FileConverter.convert(
      JSON,
      jsonFolder + "input/mock-data-2",
      CSV(inferSchema = true, header = true, ";"),
      csvFolder + "output/mock-data-2",
      SaveMode.Overwrite
    )
    val actual =
      DataFrameUtils
        .loadDataFrame(CSV(inferSchema = true, header = true, ";"), csvFolder + "output/mock-data-2")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convert orc to csv" in {
    FileConverter
      .convert(
        ORC(None),
        orcFolder + "input/mock-data-2",
        CSV(inferSchema = true, header = true, ";"),
        csvFolder + "output/mock-data-2",
        SaveMode.Overwrite
      )
    val actual =
      DataFrameUtils
        .loadDataFrame(CSV(inferSchema = true, header = true, ";"), csvFolder + "output/mock-data-2")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convert avro to csv" in {
    FileConverter.convert(
      AVRO,
      avroFolder + "input/mock-data-2",
      CSV(inferSchema = true, header = true, ";"),
      csvFolder + "output/mock-data-2",
      SaveMode.Overwrite
    )
    val actual =
      DataFrameUtils
        .loadDataFrame(CSV(inferSchema = true, header = true, ";"), csvFolder + "output/mock-data-2")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convert xml to csv" in {
    FileConverter.convert(
      XML("persons", "person"),
      xmlFolder + "input/mock-data-2",
      CSV(inferSchema = true, header = true, ";"),
      csvFolder + "output/mock-data-2",
      SaveMode.Overwrite
    )
    val actual =
      DataFrameUtils
        .loadDataFrame(CSV(inferSchema = true, header = true, ";"), csvFolder + "output/mock-data-2")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected, ignoreNullable = true)
  }

  "BasicSink.convert" should "convert xlsx to csv" in {
    FileConverter.convert(
      XLSX,
      xlsxFolder + "input/folder1/mock-xlsx-data-13.xlsx",
      CSV(inferSchema = true, header = true, ";"),
      csvFolder + "output/folder1/mock-xlsx-data-13",
      SaveMode.Overwrite
    )
    val actual =
      DataFrameUtils
        .loadDataFrame(CSV(inferSchema = true, header = true, ";"), csvFolder + "output/folder1/mock-xlsx-data-13")
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, expected2, ignoreNullable = true)
  }
}
