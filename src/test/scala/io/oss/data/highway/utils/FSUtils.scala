package io.oss.data.highway.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.MiniDFSCluster

import java.io.File
import java.nio.file.{Files, Paths}
import scala.reflect.io.Directory

case class HdfsEntity(fs: FileSystem, hdfsUri: String)

trait FSUtils {

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
      .foreach(file => {
        val path      = Paths.get(file.getPath)
        val directory = new Directory(file)
        directory.deleteRecursively()
        Files.deleteIfExists(path)
      })
  }
}
