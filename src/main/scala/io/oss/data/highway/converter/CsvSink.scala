package io.oss.data.highway.converter

import java.io.FileInputStream
import java.nio.file.{Files, Path, Paths}

import io.oss.data.highway.model.DataHighwayError.{CsvError, ReadFileError}
import io.oss.data.highway.model._
import io.oss.data.highway.utils.Constants._
import io.oss.data.highway.utils.{DataFrameUtils, FilesUtils}
import org.apache.poi.ss.usermodel.{CellType, Sheet, WorkbookFactory}
import org.apache.spark.sql.SaveMode
import cats.implicits._
import io.oss.data.highway.configuration.SparkConfig

import scala.annotation.tailrec

object CsvSink {

  /**
    * Converts file to csv
    *
    * @param in The input data path
    * @param out The generated csv file path
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @param sparkConfig The Spark Configuration
    * @return Unit if successful, otherwise Error
    */
  def convertToCsv(in: String,
                   out: String,
                   saveMode: SaveMode,
                   inputDataType: DataType,
                   sparkConfig: SparkConfig): Either[CsvError, Unit] = {
    DataFrameUtils(sparkConfig)
      .loadDataFrame(in, inputDataType)
      .map(df => {
        df.coalesce(1)
          .write
          .mode(saveMode)
          .option("inferSchema", "true")
          .option("header", "true")
          .option("sep", SEPARATOR)
          .csv(out)
      })
      .leftMap(thr => CsvError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Converts parquet files to csv files
    *
    * @param in The input data path
    * @param out The generated csv file path
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @param sparkConfig The Spark Configuration
    * @return List[Unit], otherwise Error
    */
  def handleCsvChannel(
      in: String,
      out: String,
      saveMode: SaveMode,
      inputDataType: DataType,
      sparkConfig: SparkConfig): Either[DataHighwayError, List[Unit]] = {
    for {
      folders <- FilesUtils.listFoldersRecursively(in)
      list <- folders
        .traverse(folder => {
          val suffix = FilesUtils.reversePathSeparator(folder).split("/").last
          convertToCsv(folder,
                       s"$out/$suffix",
                       saveMode,
                       inputDataType,
                       sparkConfig)
        })
        .leftMap(error =>
          CsvError(error.message, error.cause, error.stacktrace))
    } yield list
  }

  /**
    * Converts an Xlsx sheet to a CSV file.
    *
    * @param fileRelativePath The xlsx input file name with its relative path
    * @param sheet            The provided Xlsx Sheet
    * @param csvOutputFolder  The generated CSV output folder
    */
  private[converter] def convertXlsxSheetToCsvFile(
      fileRelativePath: String,
      sheet: Sheet,
      csvOutputFolder: String): Either[DataHighwayError, Path] = {
    val data = new StringBuilder
    Either
      .catchNonFatal {
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

        val fName = fileRelativePath.replaceFirst(PATH_WITHOUT_EXTENSION, EMPTY)
        createPathRecursively(s"$csvOutputFolder/$fName")
        Files.write(
          Paths.get(
            s"$csvOutputFolder/$fName/${sheet.getSheetName}.$CSV_EXTENSION"),
          data.toString.getBytes(FORMAT)
        )
      }
      .leftMap(thr =>
        ReadFileError(thr.getMessage, thr.getCause, thr.getStackTrace))
  }

  /**
    * Creates a path recusuvely
    *
    * @param path The provided path to be created
    * @return the created path
    */
  private[converter] def createPathRecursively(path: String): String = {
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
    */
  private[converter] def convertXlsxFileToCsvFiles(
      fileRelativePath: String,
      inputExcelPath: FileInputStream,
      csvOutputPath: String): Either[DataHighwayError, Unit] = {
    Either
      .catchNonFatal {
        val wb = WorkbookFactory.create(inputExcelPath)
        for (i <- 0 until wb.getNumberOfSheets) {
          convertXlsxSheetToCsvFile(fileRelativePath,
                                    wb.getSheetAt(i),
                                    csvOutputPath)
        }
        if (inputExcelPath != null) inputExcelPath.close()
      }
      .leftMap(thr => {
        if (inputExcelPath != null) inputExcelPath.close()
        ReadFileError(thr.getMessage, thr.getCause, thr.getStackTrace)
      })
  }

  /**
    * Converts Xlsx files to multiple CSV files.
    *
    * @param inputPath  The provided Excel input folder
    * @param outputPath The generated CSV output folder
    */
  def handleXlsxCsvChannel(
      inputPath: String,
      outputPath: String,
      extensions: Seq[String]): Either[DataHighwayError, List[Unit]] = {
    FilesUtils
      .getFilesFromPath(inputPath, extensions)
      .flatMap(files =>
        files.traverse(file => {
          val suffix =
            FilesUtils.reversePathSeparator(file).stripPrefix(inputPath)
          convertXlsxFileToCsvFiles(suffix,
                                    new FileInputStream(file),
                                    outputPath)
        }))
  }
}
