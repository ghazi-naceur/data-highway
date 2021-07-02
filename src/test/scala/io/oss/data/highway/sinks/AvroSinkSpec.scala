package io.oss.data.highway.sinks

import java.io.File
import java.nio.file.{Files, Paths}
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import io.oss.data.highway.models.{AVRO, CSV, JSON, Local, PARQUET}
import io.oss.data.highway.utils.DataFrameUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.reflect.io.Directory

class AvroSinkSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach with DatasetComparer {

  val folderParquetToAvro = "src/test/resources/parquet_to_avro-data/"
  val folderJsonToAvro    = "src/test/resources/json_to_avro-data/"
  val folderCsvToAvro     = "src/test/resources/csv_to_avro-data/"

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  override def beforeEach(): Unit = {
    deleteFolderWithItsContent(folderParquetToAvro)
    deleteFolderWithItsContent(folderJsonToAvro)
    deleteFolderWithItsContent(folderCsvToAvro)
  }

  private def getExpected: DataFrame = {
    import spark.implicits._
    List(
      (6.0, "Marquita", "Jarrad", "mjarrad5@rakuten.co.jp", "Female", "247.246.40.151"),
      (7.0, "Bordie", "Altham", "baltham6@hud.gov", "Male", "234.202.91.240"),
      (8.0, "Dom", "Greson", "dgreson7@somehting.com", "Male", "103.7.243.71"),
      (9.0, "Alphard", "Meardon", "ameardon8@comsenz.com", "Male", "37.31.17.200"),
      (10.0, "Reynold", "Neighbour", "rneighbour9@gravatar.com", "Male", "215.57.123.52")
    ).toDF("id", "first_name", "last_name", "email", "gender", "ip_address")
  }

  private def deleteFolderWithItsContent(path: String): Unit = {
    new File(path + "output").listFiles.toList
      .filterNot(_.getName.endsWith(".gitkeep"))
      .foreach(file => {
        val path      = Paths.get(file.getPath)
        val directory = new Directory(file)
        directory.deleteRecursively()
        Files.deleteIfExists(path)
      })
  }

  "AvroSink.saveParquetAsAvro" should "save a parquet as an avro file" in {
    AvroSink
      .convertToAvro(
        folderParquetToAvro + "input/mock-data-2",
        folderParquetToAvro + "output/mock-data-2",
        folderParquetToAvro + "processed",
        SaveMode.Overwrite,
        Local,
        PARQUET
      )
    val actual = DataFrameUtils
      .loadDataFrame(folderParquetToAvro + "output/mock-data-2", AVRO)
      .right
      .get
      .orderBy("id")
      .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  "AvroSink.saveJsonAsAvro" should "save a json as an avro file" in {
    AvroSink
      .convertToAvro(
        folderJsonToAvro + "input/mock-data-2",
        folderJsonToAvro + "output/mock-data-2",
        folderJsonToAvro + "processed",
        SaveMode.Overwrite,
        Local,
        JSON
      )
    val actual =
      DataFrameUtils
        .loadDataFrame(folderJsonToAvro + "output/mock-data-2", AVRO)
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  "AvroSink.saveCsvAsAvro" should "save a csv as an avro file" in {
    AvroSink
      .convertToAvro(
        folderCsvToAvro + "input/mock-data-2",
        folderCsvToAvro + "output/mock-data-2",
        folderCsvToAvro + "processed",
        SaveMode.Overwrite,
        Local,
        CSV
      )
    val actual =
      DataFrameUtils
        .loadDataFrame(folderCsvToAvro + "output/mock-data-2", AVRO)
        .right
        .get
        .orderBy("id")
        .select("id", "first_name", "last_name", "email", "gender", "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }
}
