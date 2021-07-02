package io.oss.data.highway.sinks

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Path, Paths}
import io.oss.data.highway.models.DataHighwayError.ReadFileError
import io.oss.data.highway.models._
import io.oss.data.highway.utils.Constants._
import io.oss.data.highway.utils.{DataFrameUtils, FilesUtils}
import org.apache.poi.ss.usermodel.{CellType, Sheet, WorkbookFactory}
import org.apache.spark.sql.SaveMode
import cats.implicits._
import org.apache.log4j.Logger

import scala.annotation.tailrec

object CsvSink {

  val logger: Logger = Logger.getLogger(CsvSink.getClass.getName)

  /**
    * Converts file to csv
    *
    * @param in The input data path
    * @param out The generated csv file path
    * @param basePath The base path for input, output and processed folders
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @return a List of Path, otherwise an Error
    */
  def convertToCsv(
      in: String,
      out: String,
      basePath: String,
      saveMode: SaveMode,
      fileSystem: FileSystem,
      inputDataType: DataType
  ): Either[Throwable, List[String]] = {
    DataFrameUtils
      .loadDataFrame(in, inputDataType)
      .map(df => {
        df.coalesce(1)
          .write
          .mode(saveMode)
          .option("inferSchema", "true")
          .option("header", "true")
          .option("sep", SEPARATOR)
          .csv(out)
        logger.info(
          s"Successfully converting '$inputDataType' data from input folder '$in' to '${CSV.getClass.getName}' and store it under output folder '$out'."
        )
      })
      .flatMap(_ => FilesUtils.movePathContent(in, basePath))
  }

  /**
    * Converts files to csv
    *
    * @param in The input data path
    * @param out The generated csv file path
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @return List of List of Path, otherwise Error
    */
  def handleCsvChannel(
      in: String,
      out: String,
      saveMode: SaveMode,
      fileSystem: FileSystem,
      inputDataType: DataType
  ): Either[Throwable, List[List[String]]] = {
    val basePath = new File(in).getParent
    for {
      folders <- FilesUtils.listFoldersRecursively(in)
      list <-
        folders
          .filterNot(path => new File(path).listFiles.filter(_.isFile).toList.isEmpty)
          .traverse(folder => {
            val suffix = FilesUtils.reversePathSeparator(folder).split("/").last
            convertToCsv(folder, s"$out/$suffix", basePath, saveMode, fileSystem, inputDataType)
          })
      _ = FilesUtils.cleanup(in)
    } yield list
  }

  /**
    * Converts an Xlsx sheet to a CSV file.
    *
    * @param fileRelativePath The xlsx input file name with its relative path
    * @param sheet            The provided Xlsx Sheet
    * @param csvOutputFolder  The generated CSV output folder
    * @return a Path, otherwise an Error
    */
  private[sinks] def convertXlsxSheetToCsvFile(
      fileRelativePath: String,
      sheet: Sheet,
      csvOutputFolder: String
  ): Either[DataHighwayError, Path] = {
    val data = new StringBuilder
    Either.catchNonFatal {
      val rowIterator = sheet.iterator
      while (rowIterator.hasNext) {
        val row = rowIterator.next
        for (index <- 0 until row.getLastCellNum) {
          if (row.getCell(index) == null) {
            data.append(EMPTY)
          } else {
            val cellType = row.getCell(index).getCellType
            val cell     = row.getCell(index)
            cellType match {
              case CellType._NONE   => data.append(cell.toString)
              case CellType.NUMERIC => data.append(cell.getNumericCellValue)
              case CellType.STRING =>
                val str =
                  cell.getStringCellValue.replaceAll("\n\r|\n|\r|\\R", EMPTY)
                data.append(str)
              case CellType.FORMULA => data.append(cell.getCellFormula)
              case CellType.BLANK   => data.append(cell.toString)
              case CellType.BOOLEAN => data.append(cell.getBooleanCellValue)
              case CellType.ERROR   => data.append(cell.toString)
            }
          }
          data.append(SEPARATOR)
        }
        data.deleteCharAt(data.length() - 1).append("\n")
      }

      val fName =
        fileRelativePath.replaceFirst(PATH_WITHOUT_EXTENSION_REGEX, EMPTY)
      createPathRecursively(s"$csvOutputFolder/$fName")
      logger.info(s"Successfully creating the path '$csvOutputFolder/$fName'.")
      Files.write(
        Paths.get(s"$csvOutputFolder/$fName/${sheet.getSheetName}.${CSV.extension}"),
        data.toString.getBytes(FORMAT)
      )
    }.leftMap(thr => ReadFileError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Creates a path recursively
    *
    * @param path The provided path to be created
    * @return The created path
    */
  private[sinks] def createPathRecursively(path: String): String = {
    val folders = path.split("/").toList

    @tailrec
    def loop(list: List[String], existingPath: String): String = {
      list match {
        case Nil => existingPath
        case head :: tail =>
          if (Files.exists(Paths.get(s"$existingPath$head"))) {
            loop(tail, s"$existingPath$head/")
          } else {
            Files
              .createDirectory(Paths.get(s"$existingPath$head"))
              .toUri
              .getPath
            loop(tail, s"$existingPath$head/")
          }
      }
    }

    loop(folders, EMPTY)
  }

  /**
    * Converts an Xlsx file to a CSV file.
    *
    * @param fileRelativePath The xlsx input file name with its relative path
    * @param inputExcelPath   The provided Excel file
    * @param csvOutputPath    The generated CSV output folder
    * @return a Unit, otherwise an Error
    */
  private[sinks] def convertXlsxFileToCsvFiles(
      fileRelativePath: String,
      inputExcelPath: FileInputStream,
      csvOutputPath: String
  ): Either[DataHighwayError, Unit] = {
    Either.catchNonFatal {
      val wb = WorkbookFactory.create(inputExcelPath)
      for (i <- 0 until wb.getNumberOfSheets) {
        convertXlsxSheetToCsvFile(fileRelativePath, wb.getSheetAt(i), csvOutputPath)
        logger.info(
          s"Successfully converting '${XLSX.getClass.getName}/${XLS.getClass.getName}' data from input folder " +
            s"'$fileRelativePath' to '${CSV.getClass.getName}' and store it under output folder '$csvOutputPath'."
        )
      }
      if (inputExcelPath != null) inputExcelPath.close()
    }.leftMap(thr => {
      if (inputExcelPath != null) inputExcelPath.close()
      ReadFileError(thr.getMessage, thr.getCause, thr.getStackTrace)
    })
  }

  /**
    * Converts Xlsx files to multiple CSV files.
    *
    * @param inputPath  The provided Excel input folder
    * @param outputPath The generated CSV output folder
    * @return a List of Unit, otherwise Error
    */
  def handleXlsxCsvChannel(
      inputPath: String,
      outputPath: String,
      fileSystem: FileSystem,
      extensions: Seq[String]
  ): Either[DataHighwayError, List[Unit]] = {
    val basePath = new File(inputPath).getParent
    FilesUtils
      .getFilesFromPath(inputPath, extensions)
      .map(files =>
        {
          files.traverse(file => {
            val suffix =
              FilesUtils.reversePathSeparator(file).stripPrefix(inputPath)
            convertXlsxFileToCsvFiles(suffix, new FileInputStream(file), outputPath)
          })
          files.traverse(file => {
            val lastFolder = file.split("/").dropRight(1).mkString("/")
            FilesUtils.movePathContent(s"$lastFolder", basePath)
          })
          FilesUtils.cleanup(inputPath)
        }.toList
      )
  }
}
