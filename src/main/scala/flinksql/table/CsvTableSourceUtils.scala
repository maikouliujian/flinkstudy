package flinksql.table

import java.io.{File, FileOutputStream, OutputStreamWriter}

import org.apache.flink.table.api.Types
import org.apache.flink.table.sources.CsvTableSource

/**
  * @author lj
  * @createDate 2020/1/21 17:10
  **/
object CsvTableSourceUtils {

  def genRatesHistorySource: CsvTableSource = {

    val csvRecords = Seq(
      "rowtime ,currency   ,rate",
      "09:00:00   ,US Dollar  , 102",
      "09:00:00   ,Euro       , 114",
      "09:00:00  ,Yen        ,   1",
      "10:45:00   ,Euro       , 116",
      "11:15:00   ,Euro       , 119",
      "11:49:00   ,Pounds     , 108"
    )
    // 测试数据写入临时文件
    val tempFilePath =
      writeToTempFile(csvRecords.mkString("$"), "csv_source_", "tmp")

    // 创建Source connector
    new CsvTableSource(
      tempFilePath,
      Array("rowtime","currency","rate"),
      Array(
        Types.STRING,Types.STRING,Types.STRING
      ),
      ",",
      "$",
      ',',
      true,
      "%",
      true
    )
  }

  def writeToTempFile(
                       contents: String,
                       filePrefix: String,
                       fileSuffix: String,
                       charset: String = "UTF-8"): String = {
    val tempFile = File.createTempFile(filePrefix, fileSuffix)
    val tmpWriter = new OutputStreamWriter(new FileOutputStream(tempFile), charset)
    tmpWriter.write(contents)
    tmpWriter.close()
    tempFile.getAbsolutePath
  }

}
