package io.oss.data.highway.utils

import java.io.FileOutputStream

import org.apache.poi.ss.usermodel.Sheet
import org.apache.poi.ss.util.CellReference
import org.apache.poi.xssf.usermodel.{
  XSSFSheet,
  XSSFTableStyleInfo,
  XSSFWorkbook
}

object MockSheetCreator {

  def createXlsxSheet(sheetName: String): Sheet = {
    val wb = new XSSFWorkbook()
    val sheet = wb.createSheet(sheetName)
    feedSheet(wb, sheet)
  }

  private def feedSheet(wb: XSSFWorkbook, sheet: XSSFSheet): XSSFSheet = {
    val reference =
      wb.getCreationHelper
        .createAreaReference(new CellReference(0, 0), new CellReference(2, 2))

    val table = sheet.createTable(reference)
    table.getCTTable.getTableColumns.getTableColumnArray(1).setId(2)
    table.getCTTable.getTableColumns.getTableColumnArray(2).setId(3)

    table.setName("table-name")
    table.setDisplayName("table-name")

    table.getCTTable.addNewTableStyleInfo()
    table.getCTTable.getTableStyleInfo.setName("table")

    val style = table.getStyle.asInstanceOf[XSSFTableStyleInfo]
    style.setName("table")
    style.setShowColumnStripes(false)
    style.setShowRowStripes(true)
    style.setFirstColumn(false)
    style.setLastColumn(false)
    style.setShowRowStripes(true)
    style.setShowColumnStripes(true)

    val row1 = sheet.createRow(0)
    val cell1 = row1.createCell(0)
    cell1.setCellValue("Field1")
    val cell2 = row1.createCell(1)
    cell2.setCellValue("Field2")
    val cell3 = row1.createCell(2)
    cell3.setCellValue("Field3")
    val cell4 = row1.createCell(3)
    cell4.setCellValue("Field4")
    val cell5 = row1.createCell(4)
    cell5.setCellValue("Field4")
    val cell6 = row1.createCell(5)
    cell6.setCellValue("Field5")

    val row2 = sheet.createRow(1)
    val cell7 = row2.createCell(0)
    cell7.setCellValue("some-value")
    val cell8 = row2.createCell(1)
    cell8.setCellValue("some-value")
    val cell9 = row2.createCell(2)
    cell9.setCellValue("some-value")
    val cell10 = row2.createCell(3)
    cell10.setCellValue("some-value")
    val cell11 = row2.createCell(4)
    cell11.setCellValue("some-value")
    val cell12 = row2.createCell(5)
    cell12.setCellValue("some-value")

    sheet
  }

  def createXlsxFile(sheetNames: List[String], path: String): Unit = {
    val wb = new XSSFWorkbook()
    sheetNames.map(sheetName => {
      createXlsxSheet(sheetName)
      val sheet = wb.createSheet(sheetName)
      feedSheet(wb, sheet)
    })
    val outputStream = new FileOutputStream(path)
    wb.write(outputStream)
  }
}
