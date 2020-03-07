package com.longi.bigdata.sparkutils

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Author: whn
  * Date: 2019-11-13 14:04
  * Version: 1.0
  * Function:
  */
object OracleUtils {

  def readFromOracle(spark: SparkSession, propName: String, sqlString: String): DataFrame = {
    val prop = PropertiesUtils.loadProperties(propName)
    val optionMap = Map("url" -> prop.getProperty("url"),
      "user" -> prop.getProperty("user"),
      "password" -> prop.getProperty("password"),
      "driver" -> prop.getProperty("driver"))
    val sqlStr = s"($sqlString) t"
    spark.read.format("jdbc")
      .options(optionMap)
      .option("dbtable", sqlStr)
      .load()
  }

  def writeToOracle(df: DataFrame, propName: String, tableName: String, saveMode: String): Unit = {
    val prop = PropertiesUtils.loadProperties(propName)
    val oraUrl = prop.getProperty("url")
    df.write.mode(saveMode).jdbc(oraUrl, tableName, prop)
  }
  def writeToOracleByLoadType(df: DataFrame, propName: String, tableName: String, dataLoadType: String): Unit = {
    val prop = PropertiesUtils.loadProperties(propName)
    val oraUrl = prop.getProperty("url")
    if (dataLoadType.equals("1")) { // 全量加载
      df.write.mode(SaveMode.Overwrite).jdbc(oraUrl, tableName, prop)
    }
    else { // 增量加载
      df.write.mode(SaveMode.Append).jdbc(oraUrl, tableName, prop)
    }
  }
}
