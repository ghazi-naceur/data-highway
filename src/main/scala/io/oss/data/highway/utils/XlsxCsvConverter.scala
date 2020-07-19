package io.oss.data.highway.utils

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Path, Paths}

import scala.annotation.tailrec

import cats.implicits._
import io.oss.data.highway.model.DataHighwayError
import io.oss.data.highway.model.DataHighwayError.CsvGenerationError
import io.oss.data.highway.utils.Constants._
import org.apache.poi.ss.usermodel.{CellType, Sheet, WorkbookFactory}

object XlsxCsvConverter {

  /**
   * Converts an Xlsx sheet to a CSV file.
   *
   * @param fileName        The xlsx input file name
   * @param sheet           The provided Xlsx Sheet
   * @param csvOutputFolder The generated CSV output folder
   */
  private[utils] def convertXlsxSheetToCsvFile(fileName: String, sheet: Sheet, csvOutputFolder: String): Either[DataHighwayError, Path] = {
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
            val cell = row.getCell(index)
            cellType match {
              case CellType._NONE => data.append(cell.toString)
              case CellType.NUMERIC => data.append(cell.getNumericCellValue)
              case CellType.STRING =>
                val str = cell.getStringCellValue.replaceAll("\n\r|\n|\r|\\R", EMPTY)
                data.append(str)
              case CellType.FORMULA => data.append(cell.getCellFormula)
              case CellType.BLANK => data.append(cell.toString)
              case CellType.BOOLEAN => data.append(cell.getBooleanCellValue)
              case CellType.ERROR => data.append(cell.toString)
            }
          }
          data.append(SEPARATOR)
        }
        data.append("\n")
      }
      val fName = fileName.split(File.separatorChar).last.replaceFirst("[.][^.]+$", EMPTY)
      createPathRecursively(s"$csvOutputFolder${File.separatorChar}$fName")
      Files.write(
        Paths.get(s"$csvOutputFolder${File.separatorChar}$fName${File.separatorChar}${sheet.getSheetName}$CSV_EXTENSION"),
        data.toString.getBytes(FORMAT)
      )
    }.leftMap(thr => CsvGenerationError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  def createPathRecursively(path: String): String = {
    val folders = path.replaceAll("\\\\", "").split("/").toList

    @tailrec
    def loop(list: List[String], existingPath: String): String = {
      list match {
        case Nil => existingPath
        case head :: tail =>
          if (Files.exists(Paths.get(s"$existingPath$head"))) {
            loop(tail, s"$existingPath$head/")
          } else {
            Files.createDirectory(Paths.get(s"$existingPath$head")).toUri.getPath
            loop(tail, s"$existingPath$head/")
          }
      }
    }

    loop(folders, "")
  }

  /**
   * Converts an Xlsx file to a CSV file.
   *
   * @param fileName       The xlsx input file name
   * @param inputExcelPath The provided Excel file
   * @param csvOutputPath  The generated CSV output folder
   */
  private[utils] def convertXlsxFileToCsvFiles(fileName: String, inputExcelPath: FileInputStream, csvOutputPath: String): Either[DataHighwayError, Unit] = {
    Either.catchNonFatal {
      val wb = WorkbookFactory.create(inputExcelPath)
      for (i <- 0 until wb.getNumberOfSheets) {
        convertXlsxSheetToCsvFile(fileName, wb.getSheetAt(i), csvOutputPath)
      }
      if (inputExcelPath != null) inputExcelPath.close()
    }.leftMap(thr => {
      if (inputExcelPath != null) inputExcelPath.close()
      CsvGenerationError(thr.getMessage, thr.getCause, thr.getStackTrace)
    })
  }

  def getFilesList(path: File): List[String] = {
    path.listFiles().toList.map(file => file.getPath).filter(name => {
      name.endsWith(XLSX_EXTENSION) || name.endsWith(XLS_EXTENSION)
    })
  }

  /**
   * Gets files' names located in a provided path
   *
   * @param path The provided path
   * @return a list of files names without the extension
   */
  private[utils] def getFilesFromPath(path: String): Either[CsvGenerationError, List[String]] = {
    Either.catchNonFatal {
      listFilesRecursively(new File(path)).map(_.getPath).toList
    }.leftMap(thr => CsvGenerationError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  final def listFilesRecursively(base: File, recursive: Boolean = true): Seq[File] = {
    val files = base.listFiles
    val result = files.filter(_.isFile).filter(file => {
      file.getPath.endsWith(XLSX_EXTENSION) || file.getPath.endsWith(XLS_EXTENSION)
    })
    result ++
      files
        .filter(_.isDirectory)
        .flatMap(listFilesRecursively(_, recursive))
  }

  def reversePathSeparator(path: String): String =
    path.replace("\\", "/")

  /**
   * Converts Xlsx files to multiple CSV files.
   *
   * @param inputPath  The provided Excel input folder
   * @param outputPath The generated CSV output folder
   */
  def apply(inputPath: String, outputPath: String): Either[DataHighwayError, List[Unit]] = {
    getFilesFromPath(inputPath).flatMap(files =>
      files.traverse(file => {
        val suffix = reversePathSeparator(file).stripPrefix(inputPath)
        convertXlsxFileToCsvFiles(suffix, new FileInputStream(file), outputPath)
      })
    )
  }
}
