package com.longi.bigdata.sparkutils

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.crealytics.spark.excel._

/**
 * Author: whn
 * Date: 2019-9-20 13:40
 * Version: 1.0
 * Function:
 */
object CsvAndExcelUtils {

  def readFromCSV(spark: SparkSession, header: Boolean, path: String, delimiter: String = ","): DataFrame = {
    spark.read.format("csv")
      .option("header", header)
      .option("multiLine", "true")
      .option("delimiter", delimiter)
      .load(path)
  }

  def writeToCSV(df: DataFrame, header: Boolean, saveMode: String, delimiter: String, path: String, coalesce: Int = 1): Unit = {
    df.coalesce(coalesce)
      .write
      .mode(saveMode)
      .option("delimiter", delimiter)
      .option("header", header)
      .csv(path)
  }

  def readFromExcel(spark: SparkSession, header: Boolean, dataAddress: String, path: String): DataFrame = {
    spark.read.excel(
      useHeader = header, // Required, 是否使用数据源第一行作为表头
      dataAddress = dataAddress, // 设置数据区域 Optional, default: "A1", 可以指定工作表"'My Sheet'!B3:C35"
      treatEmptyValuesAsNulls = true, // 是否将空的单元格设置为null, 不设置遇null则报错 Optional, default: true
      inferSchema = false // 自动推断字段类型 Optional, default: false 不推断字段类型也不自定义字段时，字段均为String
      //      maxRowsInMemory = 1000 // Optional, default None. If set, uses a streaming reader which can help with big files
      //      addColorColumns = true, // Optional, default: false
      //      timestampFormat = "yyyy-M-dd" // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
      //      excerptSize = 10, // 如果进行模式推断，推断的行数可以进行配置 Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
      //      workbookPassword = "pass", // Optional, default None. Requires unlimited strength JCE for older JVMs
      //      schema(myCustomSchema) // 自定义字段 Optional, default: Either inferred schema, or all columns are Strings
    ).load(path)
  }

  def writeToExcel(df: DataFrame, header: Boolean, dataAddress: String, saveMode: String, delimiter: String, path: String, coalesce: Int = 1): Unit = {
    df.write
      .format("com.crealytics.spark.excel")
      .option("dataAddress", dataAddress)
      .option("useHeader", header)
      //      .option("dateFormat", "yyyy-mm-dd") // Optional, default: yy-m-d h:mm
      //      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss.000
      .mode(saveMode)
      .save(path)
  }

}
