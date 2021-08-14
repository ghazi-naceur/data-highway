package io.oss.data.highway.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import java.nio.file.{Files, Paths}
import scala.reflect.io.Directory

case class HdfsEntity(fs: FileSystem, hdfsUri: String)

trait TestHelper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  val parquetFolder = "src/test/resources/data/parquet/"
  val jsonFolder    = "src/test/resources/data/json/"
  val avroFolder    = "src/test/resources/data/avro/"
  val xlsxFolder    = "src/test/resources/data/xlsx/"
  val csvFolder     = "src/test/resources/data/csv/"

  val expected: DataFrame = {
    import spark.implicits._
    List(
      (6.0, "Marquita", "Jarrad", "mjarrad5@rakuten.co.jp", "Female", "247.246.40.151"),
      (7.0, "Bordie", "Altham", "baltham6@hud.gov", "Male", "234.202.91.240"),
      (8.0, "Dom", "Greson", "dgreson7@somehting.com", "Male", "103.7.243.71"),
      (9.0, "Alphard", "Meardon", "ameardon8@comsenz.com", "Male", "37.31.17.200"),
      (10.0, "Reynold", "Neighbour", "rneighbour9@gravatar.com", "Male", "215.57.123.52")
    ).toDF("id", "first_name", "last_name", "email", "gender", "ip_address")
  }

  val expected2: DataFrame = {
    import spark.implicits._
    List(
      (1.0, "Cass", "Roux", "croux0@flavors.me", "Male", "97.27.35.122"),
      (2.0, "Tabbitha", "Richfield", "trichfield1@imageshack.us", "Female", "133.61.220.171"),
      (3.0, "Patrizia", "Dwane", "pdwane2@multiply.com", "Female", "53.91.92.116")
    ).toDF("id", "first_name", "last_name", "email", "gender", "ip_address")
  }

  val hdfsEntity: HdfsEntity = {
    val baseDir = Files.createTempDirectory("test_hdfs").toFile.getAbsoluteFile
    val conf    = new Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
    val builder     = new MiniDFSCluster.Builder(conf)
    val hdfsCluster = builder.build()

    val hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort()
    val fs      = hdfsCluster.getFileSystem
    HdfsEntity(fs, hdfsURI)
  }

  def deleteFolderWithItsContent(path: String): Unit = {
    new File(path).listFiles.toList
      .filterNot(_.getName.endsWith(".gitkeep"))
      .foreach(file => {
        val path      = Paths.get(file.getPath)
        val directory = new Directory(file)
        directory.deleteRecursively()
        Files.deleteIfExists(path)
      })
  }
}
