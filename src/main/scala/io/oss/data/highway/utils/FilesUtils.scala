package io.oss.data.highway.utils

import java.io.{File, FileWriter}
import io.oss.data.highway.model.DataHighwayError.ReadFileError
import cats.syntax.either._
import org.apache.commons.io.FileUtils

import java.nio.file.{Files, Path, StandardCopyOption}
import scala.annotation.tailrec
import scala.util.Try

object FilesUtils {

  /**
    * Gets files' names located in a provided path
    *
    * @param path The provided path
    * @return a list of files names without the extension
    */
  def getFilesFromPath(
      path: String,
      extensions: Seq[String]): Either[ReadFileError, List[String]] = {
    Either
      .catchNonFatal {
        listFilesRecursively(new File(path), extensions).map(_.getPath).toList
      }
      .leftMap(thr =>
        ReadFileError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Lists files recursively from a path
    *
    * @param path The provided path
    * @return a Seq of files
    */
  def listFilesRecursively(path: File, extensions: Seq[String]): Seq[File] = {
    val files = path.listFiles
    val result = files
      .filter(_.isFile)
      .filter(file => {
        filterByExtension(file.getPath, extensions)
      })
    result ++
      files
        .filter(_.isDirectory)
        .flatMap(f => listFilesRecursively(f, extensions))
  }

  /**
    * Checks that the provided file has an extension that belongs to the provided ones
    *
    * @param file       The provided file
    * @param extensions The provided extensions
    * @return True if the file has a valid extension, otherwise False
    */
  def filterByExtension(file: String, extensions: Seq[String]): Boolean = {
    val fileName = file.split("/").last
    extensions.contains(fileName.substring(fileName.lastIndexOf(".") + 1))
  }

  /**
    * Lists folders recursively from a path
    *
    * @param path The provided path
    * @return a Seq of folders
    */
  def listFoldersRecursively(
      path: String): Either[ReadFileError, List[String]] = {
    @tailrec
    def getFolders(path: List[File], results: List[File]): Seq[File] =
      path match {
        case head :: tail =>
          val files = head.listFiles
          val directories = files.filter(_.isDirectory)
          val updated =
            if (files.size == directories.length) results else head :: results
          getFolders(tail ++ directories, updated)
        case _ => results
      }

    Either
      .catchNonFatal {
        getFolders(new File(path) :: Nil, Nil).map(_.getPath).reverse.toList
      }
      .leftMap(thr =>
        ReadFileError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Replaces each backslash by a slash
    *
    * @param path The provided path
    * @return a path with slash as file separator
    */
  def reversePathSeparator(path: String): String =
    path.replace("\\", "/")

  /**
    * Saves content in the provided path
    * @param path The path
    * @param fileName The file name
    * @param content The file's content
    * @return Unit, otherwise Error
    */
  def save(path: String,
           fileName: String,
           content: String): Either[Throwable, Unit] = {
    Try {
      new File(path).mkdirs()
      val fileWriter = new FileWriter(new File(s"$path/$fileName"))
      fileWriter.write(content)
      fileWriter.close()
    }.toEither
      .leftMap(thr =>
        ReadFileError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Moves only files from a path
    * @param src The input path
    * @param basePath The base path
    * @param zone The destination zone name
    * @return List of Path, otherwise an Error
    */
  def movePathContent(
      src: String,
      basePath: String,
      zone: String = "processed"): Either[ReadFileError, List[Path]] = {
    Either
      .catchNonFatal {
        val srcPath = new File(src)
        val subDestFolder = s"$basePath/$zone/${srcPath.getName}"
        new File(subDestFolder).mkdirs()
        val files = srcPath.listFiles().filter(_.isFile)
        files
          .map(file => {
            Files.move(file.toPath,
                       new File(s"$subDestFolder/${file.getName}").toPath,
                       StandardCopyOption.ATOMIC_MOVE)
          })
          .toList
      }
      .leftMap(thr =>
        ReadFileError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Deletes folder
    * @param in The folder to be deleted
    * @return Array of Unit
    */
  def deleteFolder(in: String): Array[Unit] = {
    new File(in)
      .listFiles()
      .filter(_.isDirectory)
      .map(folder => FileUtils.forceDelete(folder))
  }
}
