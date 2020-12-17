package io.oss.data.highway.utils

import java.io.{File, FileWriter}

import io.oss.data.highway.model.DataHighwayError.ReadFileError
import cats.syntax.either._

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
    Either
      .catchNonFatal {
        getRecursiveListOfFiles(new File(path)).map(_.getPath)
      }
      .leftMap(thr =>
        ReadFileError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  private def getRecursiveListOfFiles(path: File): List[File] = {
    if (path.isDirectory) {
      List(path) ++ path
        .listFiles()
        .filter(_.isDirectory)
        .flatMap(getRecursiveListOfFiles)
    } else List(path)
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
}
