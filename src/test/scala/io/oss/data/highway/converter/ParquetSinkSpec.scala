package io.oss.data.highway.converter

import java.io.File
import java.nio.file.{Files, Paths}

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import io.oss.data.highway.model.{AVRO, CSV, JSON, PARQUET}
import io.oss.data.highway.utils.DataFrameUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.reflect.io.Directory

class ParquetSinkSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterEach
    with DatasetComparer {

  val folderCsvToParquet = "src/test/resources/csv_to_parquet-data/"
  val folderJsonToParquet = "src/test/resources/json_to_parquet-data/"
  val folderAvroToParquet = "src/test/resources/avro_to_parquet-data/"
  val getExpected: DataFrame = {
    import spark.implicits._
    List(
      (6.0,
       "Marquita",
       "Jarrad",
       "mjarrad5@rakuten.co.jp",
       "Female",
       "247.246.40.151"),
      (7.0, "Bordie", "Altham", "baltham6@hud.gov", "Male", "234.202.91.240"),
      (8.0, "Dom", "Greson", "dgreson7@somehting.com", "Male", "103.7.243.71"),
      (9.0,
       "Alphard",
       "Meardon",
       "ameardon8@comsenz.com",
       "Male",
       "37.31.17.200"),
      (10.0,
       "Reynold",
       "Neighbour",
       "rneighbour9@gravatar.com",
       "Male",
       "215.57.123.52")
    ).toDF("id", "first_name", "last_name", "email", "gender", "ip_address")
  }
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  override def beforeEach(): Unit = {
    deleteFolderWithItsContent(folderCsvToParquet)
    deleteFolderWithItsContent(folderJsonToParquet)
    deleteFolderWithItsContent(folderAvroToParquet)
  }

  private def deleteFolderWithItsContent(path: String): Unit = {
    new File(path + "output").listFiles.toList
      .filterNot(_.getName.endsWith(".gitkeep"))
      .foreach(file => {
        val path = Paths.get(file.getPath)
        val directory = new Directory(file)
        directory.deleteRecursively()
        Files.deleteIfExists(path)
      })
  }

  "ParquetSink.saveCsvAsParquet" should "save a csv as a parquet file" in {
    ParquetSink
      .convertToParquet(folderCsvToParquet + "input/mock-data-2",
                        folderCsvToParquet + "output/mock-data-2",
                        folderCsvToParquet + "processed",
                        SaveMode.Overwrite,
                        CSV)
    val actual =
      DataFrameUtils
        .loadDataFrame(folderCsvToParquet + "output/mock-data-2", PARQUET)
        .right
        .get
        .orderBy("id")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  "ParquetSink.saveAvroAsParquet" should "save a avro as a parquet file" in {
    ParquetSink
      .convertToParquet(folderAvroToParquet + "input/mock-data-2",
                        folderAvroToParquet + "output/mock-data-2",
                        folderAvroToParquet + "processed",
                        SaveMode.Overwrite,
                        AVRO)
    val actual =
      DataFrameUtils
        .loadDataFrame(folderAvroToParquet + "output/mock-data-2", PARQUET)
        .right
        .get
        .orderBy("id")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }

  "ParquetSink.saveJsonAsParquet" should "save a json as a parquet file" in {
    ParquetSink
      .convertToParquet(folderJsonToParquet + "input/mock-data-2",
                        folderJsonToParquet + "output/mock-data-2",
                        folderJsonToParquet + "processed",
                        SaveMode.Overwrite,
                        JSON)
    val actual =
      DataFrameUtils
        .loadDataFrame(folderJsonToParquet + "output/mock-data-2", PARQUET)
        .right
        .get
        .orderBy("id")
        .select("id",
                "first_name",
                "last_name",
                "email",
                "gender",
                "ip_address")

    assertSmallDatasetEquality(actual, getExpected, ignoreNullable = true)
  }
}
