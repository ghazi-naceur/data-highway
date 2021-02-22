package io.oss.data.highway.converter

import io.oss.data.highway.model.{AVRO, DataType}
import io.oss.data.highway.utils.{DataFrameUtils, FilesUtils}
import org.apache.spark.sql.SaveMode
import cats.implicits._
import org.apache.log4j.Logger

import java.io.File
import java.nio.file.Path

object AvroSink {

  val logger: Logger = Logger.getLogger(AvroSink.getClass.getName)

  /**
    * Converts file to avro
    *
    * @param in The input data path
    * @param out The generated avro file path
    * @param basePath The base path for input, output and processed folders
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @return a List of Path, otherwise an Error
    */
  def convertToAvro(
      in: String,
      out: String,
      basePath: String,
      saveMode: SaveMode,
      inputDataType: DataType): Either[Throwable, List[Path]] = {
    DataFrameUtils
      .loadDataFrame(in, inputDataType)
      .map(df => {
        df.write
          .format(AVRO.extension)
          .mode(saveMode)
          .save(out)
        logger.info(
          s"Successfully converting '$inputDataType' data from input folder '$in' to '${AVRO.getClass.getName}' and store it under output folder '$out'.")
      })
      .flatMap(_ => FilesUtils.movePathContent(in, basePath))
  }

  /**
    * Converts files to avro
    *
    * @param in The input data path
    * @param out The generated avro file path
    * @param saveMode The file saving mode
    * @param inputDataType The type of the input data
    * @return List of List of Path, otherwise an Error
    */
  def handleAvroChannel(
      in: String,
      out: String,
      saveMode: SaveMode,
      inputDataType: DataType): Either[Throwable, List[List[Path]]] = {
    val basePath = new File(in).getParent
    for {
      folders <- FilesUtils.listFoldersRecursively(in)
      list <- folders
        .filterNot(path =>
          new File(path).listFiles.filter(_.isFile).toList.isEmpty)
        .traverse(folder => {
          val suffix = FilesUtils.reversePathSeparator(folder).split("/").last
          convertToAvro(folder,
                        s"$out/$suffix",
                        basePath,
                        saveMode,
                        inputDataType)
        })
      _ = FilesUtils.deleteFolder(in)
    } yield list
  }
}
