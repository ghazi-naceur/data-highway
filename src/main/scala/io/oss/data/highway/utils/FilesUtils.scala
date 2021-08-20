package io.oss.data.highway.utils

import java.io.{File, FileWriter}
import io.oss.data.highway.models.DataHighwayError.DataHighwayFileError
import cats.syntax.either._
import io.oss.data.highway.models.XLSX
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger

import java.nio.file.Files
import scala.annotation.tailrec
import scala.io.Source
import scala.util.Try

object FilesUtils {

  val logger: Logger = Logger.getLogger(FilesUtils.getClass)

  /**
    * Lists files recursively from a path
    *
    * @param path The provided path
    * @param extension The input extension used to filter initial data
    * @return a Seq of files
    */
  def listFilesRecursively(path: File, extension: String): Seq[File] = {
    val files = path.listFiles
    val result = files
      .filter(_.isFile)
      .filter(file => {
        filterByExtension(file.getPath, extension)
      })
    result ++
      files
        .filter(_.isDirectory)
        .flatMap(f => listFilesRecursively(f, extension))
  }

  /**
    * Lists files inside a list of folders
    *
    * @param folders The input folders
    * @return List of File, otherwise a Throwable
    */
  def listFiles(folders: List[String]): Either[Throwable, List[File]] = {
    Either.catchNonFatal {
      folders.flatMap(subfolder => {
        new File(subfolder).listFiles
      })
    }.leftMap(thr => DataHighwayFileError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Checks that the provided file has an extension that belongs to the provided ones
    *
    * @param file       The provided file
    * @param extension The input extension used to filter initial data
    * @return True if the file has a valid extension, otherwise False
    */
  def filterByExtension(file: String, extension: String): Boolean = {
    val fileName = file.split("/").last
    extension.equals(fileName.substring(fileName.lastIndexOf(".") + 1))
  }

  /**
    * Lists non-empty folders (contains at least 1 file) recursively from a path
    *
    * @param path The provided path
    * @return a List of String, otherwise an Error
    */
  def listNonEmptyFoldersRecursively(path: String): Either[DataHighwayFileError, List[String]] = {
    @tailrec
    def getFolders(path: List[File], results: List[File]): Seq[File] =
      path match {
        case head :: tail =>
          val files       = head.listFiles
          val directories = files.filter(_.isDirectory)
          val updated =
            if (files.size == directories.length) results else head :: results
          getFolders(tail ++ directories, updated)
        case _ => results
      }

    Either.catchNonFatal {
      getFolders(new File(path) :: Nil, Nil).map(_.getPath).reverse.toList
    }.leftMap(thr => DataHighwayFileError(thr.getMessage, thr.getCause, thr.getStackTrace))
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
    * Creates file
    *
    * @param path The path
    * @param fileName The file name
    * @param content The file's content
    * @return Unit, otherwise Error
    */
  def createFile(path: String, fileName: String, content: String): Either[Throwable, Unit] = {
    Try {
      new File(path).mkdirs()
      val fileWriter = new FileWriter(new File(s"$path/$fileName"))
      fileWriter.write(content)
      fileWriter.close()
    }.toEither
      .leftMap(thr => DataHighwayFileError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Moves files from source to destination path
    * if source is a file :
    *   input/dataset/file.txt ===> processed/file.txt
    * if source is a folder :
    *   input/dataset ===> processed/dataset
    *
    * @param src The input path
    * @param processedFolder The sub destination path
    * @return List of String, otherwise an Error
    */
  def movePathContent(
      src: String,
      processedFolder: String
  ): Either[Throwable, List[String]] = {
    Either.catchNonFatal {
      if (new File(src).isFile) {
        val srcFileName = new File(src).getName
        Files.createDirectories(new File(processedFolder).toPath)
        if (new File(s"$processedFolder/$srcFileName").exists()) {
          FileUtils.forceDelete(new File(s"$processedFolder/$srcFileName"))
          FileUtils.moveFile(new File(src), new File(s"$processedFolder/$srcFileName"))
        } else
          FileUtils.moveFile(new File(src), new File(s"$processedFolder/$srcFileName"))
        List(processedFolder)
      } else {
        val subfolderName = new File(src).getName
        if (new File(s"$processedFolder/$subfolderName").exists()) {
          FileUtils.deleteDirectory(new File(s"$processedFolder/$subfolderName"))
          FileUtils.moveDirectoryToDirectory(new File(src), new File(processedFolder), false)
        } else {
          FileUtils.moveDirectoryToDirectory(new File(src), new File(processedFolder), true)
        }
        List(processedFolder)
      }

    }.leftMap(thr => DataHighwayFileError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Cleanups a folder
    *
    * @param in The folder to be cleaned
    * @return Array of Unit
    */
  def cleanup(in: String): Array[Unit] = {
    new File(in)
      .listFiles()
      .map(FileUtils.forceDelete)
  }

  /**
    * Get lines from json file
    *
    * @param jsonPath The json file
    * @return an Iterator of String
    */
  def getLines(jsonPath: String): Iterator[String] = {
    val jsonFile = Source.fromFile(jsonPath)
    jsonFile.getLines
  }

  /**
    * Filters non-empty folders
    *
    * @param folders THe provided folders
    * @return a List of String, otherwise a Throwable
    */
  def filterNonEmptyFolders(folders: List[String]): Either[Throwable, List[String]] = {
    Either.catchNonFatal {
      folders.filterNot(path => new File(path).listFiles.filter(_.isFile).toList.isEmpty)
    }.leftMap(thr => DataHighwayFileError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Gets the file name without extension and its parent folder
    *
    * @param path The file path
    * @return String
    */
  def getFileNameAndParentFolderFromPath(
      path: String,
      extension: String = XLSX.extension
  ): String = {
    reversePathSeparator(path)
      .split("/")
      .takeRight(2)
      .mkString("/")
      .replace(s".$extension", "")
  }

  /**
    * Deletes path
    *
    * @param path The path to be deleted
    * @return Unit, otherwise an Throwable
    */
  def delete(path: String): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      FileUtils.forceDelete(new File(path))
    }
  }
}
