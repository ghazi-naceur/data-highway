package io.oss.data.highway.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}

import java.io.{BufferedWriter, File, OutputStreamWriter}
import scala.annotation.tailrec
import cats.implicits._
import io.oss.data.highway.configs.{ConfigLoader, HadoopConfigs}
import io.oss.data.highway.models.DataHighwayError.HdfsError

import java.nio.charset.StandardCharsets
import pureconfig.generic.auto._

trait HdfsUtils {
  val hadoopConf: HadoopConfigs = ConfigLoader().loadConfigs[HadoopConfigs]("hadoop")
  val conf: Configuration       = new Configuration()
  conf.set("fs.defaultFS", HdfsUtils.hadoopConf.host)
  val fs: FileSystem = FileSystem.get(conf)
}

object HdfsUtils extends HdfsUtils {

  /**
    * Saves a content inside a file
    *
    * @param fs The provided File System
    * @param file The file path
    * @param content The content to be saved
    * @return Unit, otherwise a Throwable
    */
  def save(fs: FileSystem, file: String, content: String): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      val hdfsWritePath      = new Path(file)
      val fsDataOutputStream = fs.create(hdfsWritePath, true)
      val bufferedWriter =
        new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8))
      bufferedWriter.write(content)
      bufferedWriter.close()
    }.leftMap(thr => HdfsError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Creates a folder
    *
    * @param fs The provided File System
    * @param folder The folder to be created
    * @return Boolean, otherwise a Throwable
    */
  def mkdir(fs: FileSystem, folder: String): Either[Throwable, Boolean] = {
    Either.catchNonFatal {
      fs.mkdirs(new Path(folder))
    }.leftMap(thr => HdfsError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Move files
    *
    * @param fs The provided File System
    * @param src The source folder
    * @param dest The destination folder
    * @return Boolean, otherwise a Throwable
    */
  def move(fs: FileSystem, src: String, dest: String): Either[Throwable, Boolean] = {
    Either.catchNonFatal {
      fs.rename(new Path(src), new Path(dest))
    }.leftMap(thr => HdfsError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Cleanups a folder
    *
    * @param fs The provided File System
    * @param folder The folder to be cleaned
    * @return Boolean, otherwise a Throwable
    */
  def cleanup(fs: FileSystem, folder: String): Either[Throwable, Boolean] = {
    Either.catchNonFatal {
      fs.delete(new Path(folder), true)
      fs.mkdirs(new Path(folder))
    }.leftMap(thr => HdfsError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Lists sub-folders inside a parent path
    *
    * @param fs The provided File System
    * @param path The provided parent folder
    * @return List of String, otherwise a Throwable
    */
  def listFolders(fs: FileSystem, path: String): Either[Throwable, List[String]] = {
    Either.catchNonFatal {
      fs.listStatus(new Path(path))
        .filter(_.isDirectory)
        .map(_.getPath.toUri.toString)
        .toList
    }.leftMap(thr => HdfsError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Lists files inside a folder
    *
    * @param fs The provided File System
    * @param path The provided folder
    * @return LIst of String
    */
  def listFiles(fs: FileSystem, path: String): List[String] = {
    val iterator = fs.listFiles(new Path(path), true)

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
    *
    * @param fs The provided File System
    * @param src The input path
    * @param basePath The base path
    * @param zone The destination zone name
    * @return List of Path, otherwise an Error
    */
  def movePathContent(
      fs: FileSystem,
      src: String,
      basePath: String,
      zone: String = "processed"
  ): Either[Throwable, List[String]] = {
    Either.catchNonFatal {
      if (fs.getFileStatus(new Path(src)).isFile) {
        val subDestFolder = s"$basePath/$zone/${src.split("/").takeRight(2).mkString("/")}"
        HdfsUtils.mkdir(
          fs,
          getPathWithoutUriPrefix(subDestFolder.split("/").dropRight(1).mkString("/"))
        )
        HdfsUtils.move(fs, src, getPathWithoutUriPrefix(subDestFolder))
        List(subDestFolder)
      } else {
        val srcPath       = new File(src)
        val subDestFolder = s"$basePath/$zone/${srcPath.getName}"
        HdfsUtils.mkdir(fs, getPathWithoutUriPrefix(subDestFolder))
        val files = HdfsUtils.listFiles(fs, src)
        files
          .map(file => {
            HdfsUtils.move(
              fs,
              getPathWithoutUriPrefix(file),
              s"${getPathWithoutUriPrefix(subDestFolder)}/${file.split("/").last}"
            )
            s"$subDestFolder/${file.split("/").last}"
          })
      }
    }.leftMap(thr => HdfsError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Gets the path without the URI prefix 'hdfs://host:port'
    *
    * @param path The provided path
    * @return String
    */
  def getPathWithoutUriPrefix(path: String): String = {
    "/" + path.replace("//", "/").split("/").drop(2).mkString("/")
  }

  /**
    * Filters non-empty folders
    *
    * @param fs The provided File System
    * @param folders The provided folders
    * @return List of String, otherwise a Throwable
    */
  def filterNonEmptyFolders(
      fs: FileSystem,
      folders: List[String]
  ): Either[Throwable, List[String]] = {
    Either.catchNonFatal {
      folders.filter(folder => fs.listFiles(new Path(folder), false).hasNext)
    }.leftMap(thr => HdfsError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Lists files recursively from a path
    *
    * @param fs The provided File System
    * @param hdfsPath The provided folder
    * @return List of String
    */
  def listFilesRecursively(fs: FileSystem, hdfsPath: String): List[String] = {
    fs.listStatus(new Path(hdfsPath))
      .flatMap { status =>
        if (status.isFile)
          List(status.getPath.toUri.getPath)
        else
          listFilesRecursively(fs, status.getPath.toUri.getPath)
      }
      .toList
      .sorted
  }

  /**
    * Gets json content from a json file
    *
    * @param fs The provided File System
    * @param jsonPath The provided json file path
    * @return List of String
    */
  // todo to be deleted
  def getLines(fs: FileSystem, jsonPath: String): List[String] = {
    val path   = new Path(jsonPath)
    val stream = fs.open(path)
    Stream
      .cons(stream.readLine, Stream.continually(stream.readLine))
      .takeWhile(_ != null)
      .toList
  }

  /**
    * Deletes a path
    *
    * @param fs The provided File System
    * @param path The path to be deleted
    * @return Boolean, otherwise a Throwable
    */
  def delete(fs: FileSystem, path: String): Either[Throwable, Boolean] = {
    Either.catchNonFatal {
      fs.delete(new Path(path), true)
    }
  }
}
