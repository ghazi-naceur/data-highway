package io.oss.data.highway.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}

import java.io.File
import scala.annotation.tailrec
import scala.util.Try
import cats.implicits._
import io.oss.data.highway.models.DataHighwayError.ReadFileError

object HdfsUtils {

  val conf = new Configuration()
  conf.set("fs.defaultFS", "hdfs://localhost:9000") // todo to be externalized
  val fs: FileSystem = FileSystem.get(conf)

  def mkdir(folder: String): Either[Throwable, Boolean] = {
    Try {
      fs.mkdirs(new Path(folder))
    }.toEither
  }

  def move(src: String, dest: String): Either[Throwable, Boolean] = {
    Either.catchNonFatal {
      fs.rename(new Path(src), new Path(dest))
    }.leftMap(thr => ReadFileError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  def cleanup(folder: String): Either[Throwable, Boolean] = {
    Try {
      fs.delete(new Path(folder), true)
      fs.mkdirs(new Path(folder))
    }.toEither
  }

  def isEmpty(folder: String): Either[Throwable, Boolean] = {
    Try {
      fs.listFiles(new Path(folder), false).hasNext
    }.toEither
  }

  def listFolders(path: String): Either[Throwable, List[String]] = {
    Try {
      fs.listStatus(new Path(path))
        .filter(_.isDirectory)
        .map(_.getPath.toUri.toString)
        .toList
    }.toEither
  }

  def listFiles(path: String): List[String] = {
    val iterator = HdfsUtils.fs.listFiles(new Path(path), true)

    @tailrec
    def iterate(iterator: RemoteIterator[LocatedFileStatus], acc: List[String]): List[String] = {
      if (iterator.hasNext) {
        val uri = iterator.next.getPath.toUri.toString
        iterate(iterator, uri :: acc)
      } else {
        acc
      }
    }
    iterate(iterator, List.empty[String])
  }

  /**
    * Moves files from a path to another
    * @param src The input path
    * @param basePath The base path
    * @param zone The destination zone name
    * @return List of Path, otherwise an Error
    */
  def movePathContent(
      src: String,
      basePath: String,
      zone: String = "processed"
  ): Either[ReadFileError, List[String]] = {
    Either.catchNonFatal {
      val srcPath       = new File(src)
      val subDestFolder = s"$basePath/$zone/${srcPath.getName}"
      HdfsUtils.mkdir(pathWithoutUriPrefix(subDestFolder))
      val files = HdfsUtils.listFiles(src)
      files
        .map(file => {
          HdfsUtils.move(
            pathWithoutUriPrefix(file),
            s"${pathWithoutUriPrefix(subDestFolder)}/${file.split("/").last}"
          )
          s"$subDestFolder/${file.split("/").last}"
        })
    }.leftMap(thr => ReadFileError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  def pathWithoutUriPrefix(path: String): String = {
    "/" + path.replace("//", "/").split("/").drop(2).mkString("/")
  }
}
