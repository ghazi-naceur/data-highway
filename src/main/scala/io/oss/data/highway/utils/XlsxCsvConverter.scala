package io.oss.data.highway.utils

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Path, Paths}

import cats.implicits._
import io.oss.data.highway.model.DataHighwayError
import io.oss.data.highway.model.DataHighwayError.{CsvGenerationError, PathNotFound}
import io.oss.data.highway.utils.Constants.{EMPTY, EXTENSION, FORMAT, SEPARATOR}
import org.apache.poi.ss.usermodel.{CellType, Sheet, WorkbookFactory}

object XlsxCsvConverter {

  /**
   * Converts an Xlsx sheet to a CSV file.
   *
   * @param sheet           The provided Xlsx Sheet
   * @param csvOutputFolder The generated CSV output folder
   */
  private[utils] def convertXlsxSheetToCsvFile(sheet: Sheet, csvOutputFolder: String): Either[DataHighwayError, Path] = {
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
      Files.write(
        Paths.get(s"$csvOutputFolder/${sheet.getSheetName}$EXTENSION"),
        data.toString.getBytes(FORMAT)
      )
    }.leftMap(thr => CsvGenerationError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
   * Converts an Xlsx file to a CSV file.
   *
   * @param inputExcelPath The provided Excel file
   * @param csvOutputPath  The generated CSV output folder
   */
  private[utils] def convertXlsxFileToCsvFiles(inputExcelPath: FileInputStream, csvOutputPath: String): Either[DataHighwayError, Unit] = {
    Either.catchNonFatal {
      val wb = WorkbookFactory.create(inputExcelPath)
      for (i <- 0 until wb.getNumberOfSheets) {
        convertXlsxSheetToCsvFile(wb.getSheetAt(i), csvOutputPath)
      }
      if (inputExcelPath != null) inputExcelPath.close()
    }.leftMap(thr => {
      if (inputExcelPath != null) inputExcelPath.close()
      CsvGenerationError(thr.getMessage, thr.getCause, thr.getStackTrace)
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
      val d = new File(path)
      if (d.exists && d.isDirectory)
        d.listFiles.toList.map(file => file.getPath).filter(name => name.endsWith(".xlsx") || name.endsWith(".xls"))
      else if (d.exists && d.isFile && (path.endsWith(".xlsx") || path.endsWith(".xls")))
        List(d.getPath)
      else
        throw PathNotFound(path)
    }.leftMap(thr => CsvGenerationError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
   * Converts Xlsx files to multiple CSV files.
   *
   * @param inputPath  The provided Excel input folder
   * @param outputPath The generated CSV output folder
   */
  def apply(inputPath: String, outputPath: String): Either[DataHighwayError, List[Unit]] = {
    getFilesFromPath(inputPath).flatMap(files =>
      files.traverse(file => {
        convertXlsxFileToCsvFiles(new FileInputStream(file), outputPath)
      })
    )
  }
}
