package io.oss.data.highway.utils

import java.io.File
import java.nio.file.{Files, Paths}

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.reflect.io.Directory

class CsvHandlerSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterEach
    with DatasetComparer {

  val folder = "src/test/resources/parquet_to_csv-data/"

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  override def beforeEach(): Unit = {
    new File(folder + "output").listFiles.toList
      .filterNot(_.getName.endsWith(".gitkeep"))
      .foreach(file => {
        val path = Paths.get(file.getPath)
        val directory = new Directory(file)
        directory.deleteRecursively()
        Files.deleteIfExists(path)
      })
  }

  "CsvHandler.saveCsvAsParquet" should "save a parquet as a csv file" in {
    import spark.implicits._

    CsvHandler
      .saveParquetAsCsv(folder + "input/mock-data-2",
                        folder + "output/mock-data-2",
                        ";",
                        SaveMode.Overwrite)
    val actual = CsvHandler.readParquet(folder + "output/mock-data-2", ";")

    val expected = List(
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

    assertSmallDatasetEquality(actual.right.get.orderBy("id"),
                               expected,
                               ignoreNullable = true)
  }
}
