package io.oss.data.highway.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}

import java.io.File
import scala.annotation.tailrec
import cats.implicits._
import io.oss.data.highway.configs.{ConfigLoader, HadoopConfigs}
import io.oss.data.highway.models.DataHighwayError.HdfsError

trait HdfsUtils {
  val hadoopConf: HadoopConfigs = ConfigLoader().loadHadoopConf()
}

object HdfsUtils extends HdfsUtils {

  val conf = new Configuration()
  conf.set("fs.defaultFS", HdfsUtils.hadoopConf.host)

  val fs: FileSystem = FileSystem.get(conf)

  def mkdir(folder: String): Either[Throwable, Boolean] = {
    Either.catchNonFatal {
      fs.mkdirs(new Path(folder))
    }.leftMap(thr => HdfsError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  def move(src: String, dest: String): Either[Throwable, Boolean] = {
    Either.catchNonFatal {
      fs.rename(new Path(src), new Path(dest))
    }.leftMap(thr => HdfsError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  def cleanup(folder: String): Either[Throwable, Boolean] = {
    Either.catchNonFatal {
      fs.delete(new Path(folder), true)
      fs.mkdirs(new Path(folder))
    }.leftMap(thr => HdfsError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  def listFolders(path: String): Either[Throwable, List[String]] = {
    Either.catchNonFatal {
      fs.listStatus(new Path(path))
        .filter(_.isDirectory)
        .map(_.getPath.toUri.toString)
        .toList
    }.leftMap(thr => HdfsError(thr.getMessage, thr.getCause, thr.getStackTrace))
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
  ): Either[HdfsError, List[String]] = {
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
    }.leftMap(thr => HdfsError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  def pathWithoutUriPrefix(path: String): String = {
    "/" + path.replace("//", "/").split("/").drop(2).mkString("/")
  }

  def verifyNotEmpty(folders: List[String]): Either[Throwable, List[String]] = {
    Either.catchNonFatal {
      folders.filter(folder => HdfsUtils.fs.listFiles(new Path(folder), false).hasNext)
    }.leftMap(thr => HdfsError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }
}
