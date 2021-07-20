package io.oss.data.highway.utils

import java.io.File
import java.nio.file.{Files, Paths}
import scala.reflect.io.Directory

trait FSUtils {
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
