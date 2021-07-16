package io.oss.data.highway.utils

import java.io.{File, FileWriter}
import io.oss.data.highway.models.DataHighwayError.ReadFileError
import cats.syntax.either._
import io.oss.data.highway.models.{DataType, XLSX}
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger

import java.nio.file.{Files, StandardCopyOption}
import scala.annotation.tailrec
import scala.io.Source
import scala.util.Try

object FilesUtils {

  val logger: Logger = Logger.getLogger(FilesUtils.getClass)

  /**
    * Gets files' names located in a provided path
    *
    * @param path The provided path
    * @param extensions a Sequence of extensions
    * @return a list of files names without the extension, otherwise an Error
    */
  @deprecated("used only in tests")
  def getFilesFromPath(
      path: String,
      extensions: Seq[String]
  ): Either[ReadFileError, List[String]] = {
    Either.catchNonFatal {
      listFilesRecursively(new File(path), extensions).map(_.getPath).toList
    }.leftMap(thr => ReadFileError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Lists files recursively from a path
    *
    * @param path The provided path
    * @param extensions a Sequence of extensions
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
    }.leftMap(thr => ReadFileError(thr.getMessage, thr.getCause, thr.getStackTrace))
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
    * @return a List of String, otherwise an Error
    */
  def listFoldersRecursively(path: String): Either[ReadFileError, List[String]] = {
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
    }.leftMap(thr => ReadFileError(thr.getMessage, thr.getCause, thr.getStackTrace))
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
    *
    * @param path The path
    * @param fileName The file name
    * @param content The file's content
    * @return Unit, otherwise Error
    */
  def save(path: String, fileName: String, content: String): Either[Throwable, Unit] = {
    Try {
      new File(path).mkdirs()
      val fileWriter = new FileWriter(new File(s"$path/$fileName"))
      fileWriter.write(content)
      fileWriter.close()
    }.toEither
      .leftMap(thr => ReadFileError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Moves files to processed zone
    *
    * @param src The input path
    * @param basePath The base path
    * @param zone The destination zone name, "processed" by default
    * @return List of String, otherwise an Error
    */
  def movePathContent(
      src: String,
      basePath: String,
      inputDataType: DataType,
      zone: String = "processed"
  ): Either[Throwable, List[String]] = {
    Either.catchNonFatal {
      if (new File(src).isFile) {
        val srcPath     = new File(src).getParentFile
        val srcFileName = new File(src).getName

        val subDestFolder = inputDataType match {
          case XLSX =>
            s"$basePath/$zone/${srcPath.toURI.getPath.split("/").takeRight(2).mkString("/")}"
          case _ =>
            s"$basePath/$zone/${srcPath.getName}"
        }

//        FileUtils.forceMkdir(new File(subDestFolder))
        Files.createDirectories(new File(subDestFolder).toPath)
        Files.move(
          new File(src).toPath,
          new File(subDestFolder + "/" + srcFileName).toPath,
          StandardCopyOption.REPLACE_EXISTING
        )
        List(subDestFolder)
      } else {
        val srcPath = new File(src)
        val subDestFolder = inputDataType match {
          case XLSX =>
            s"$basePath/$zone/${srcPath.toURI.getPath.split("/").takeRight(2).mkString("/")}"
          case _ =>
            s"$basePath/$zone/${srcPath.getName}"
        }

//        FileUtils.forceMkdir(new File(subDestFolder))
        Files.createDirectories(new File(subDestFolder).toPath)
        Files.move(
          new File(src).toPath,
          new File(subDestFolder).toPath,
          StandardCopyOption.REPLACE_EXISTING
        )
        List(subDestFolder)
      }

    }.leftMap(thr => ReadFileError(thr.getMessage, thr.getCause, thr.getStackTrace))
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
  def getJsonLines(jsonPath: String): Iterator[String] = {
    val jsonFile = Source.fromFile(jsonPath)
    jsonFile.getLines
  }

  /**
    * Filters non-empty folders
    *
    * @param folders THe provided folders
    * @return a List of String, otherwise a Throwable
    */
  def verifyNotEmpty(folders: List[String]): Either[Throwable, List[String]] = {
    Either.catchNonFatal {
      folders.filterNot(path => new File(path).listFiles.filter(_.isFile).toList.isEmpty)
    }.leftMap(thr => ReadFileError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Gets the file name and its parent folder
    *
    * @param path The file path
    * @return String
    */
  def getFileNameAndParentFolderFromPath(path: String): String = {
    reversePathSeparator(path)
      .split("/")
      .takeRight(2)
      .mkString("/")
      .replace(".xlsx", "")
  }
}
