package io.oss.data.highway.utils

import java.io.File

import io.oss.data.highway.model.DataHighwayError.ReadFileError
import cats.syntax.either._

object FilesUtils {

  /**
    * Gets files' names located in a provided path
    *
    * @param path The provided path
    * @return a list of files names without the extension
    */
  private[utils] def getFilesFromPath(
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
  private[utils] def listFilesRecursively(
      path: File,
      extensions: Seq[String]): Seq[File] = {
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
  private[utils] def filterByExtension(file: String,
                                       extensions: Seq[String]): Boolean = {
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
}
