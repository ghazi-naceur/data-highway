package io.oss.data.highway.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{
  FSDataOutputStream,
  FileSystem,
  FileUtil,
  LocatedFileStatus,
  Path,
  RemoteIterator
}

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
//      fs.rename(new Path(src), new Path(dest))
      fs.rename(new Path(src), new Path(dest))
//      FileUtil.copy(fs, new Path(src), fs, new Path(dest), true, conf)
//      FileUtil.replaceFile(new File(src), new File(dest))
    }.leftMap(thr => ReadFileError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  def rmdir(folder: String): Either[Throwable, Boolean] = {
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
    Try { // todo returns only first level folders
      fs.listStatus(new Path(path))
        .filter(_.isDirectory)
        .map(_.getPath.toUri.toString)
        .toList
    }.toEither
  }

  def listFiles(path: String): List[String] = {
//    val iterator = HdfsUtils.fs.listFiles(new Path(path), true)
//
//    @tailrec
//    def iterate(iterator: RemoteIterator[LocatedFileStatus], acc: List[String]): List[String] = {
//      if (iterator.next.isFile) {
//        val uri = iterator.next.getPath.toUri.toString
//        iterate(iterator, uri :: acc)
//      } else {
//        acc
//      }
//    }
//    iterate(iterator, List.empty[String])

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
}
