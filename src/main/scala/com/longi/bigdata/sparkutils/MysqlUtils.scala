package com.longi.bigdata.sparkutils

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: whn
  * Date: 2019-11-13 14:04
  * Version: 1.0
  * Function:  spark-mysql的读写
  */
object MysqlUtils {
  def readFromMysql(spark: SparkSession, propName: String, sqlString: String): DataFrame = {
    val prop = PropertiesUtils.loadProperties(propName)
    val optionMap = Map(
      "url" -> prop.getProperty("url"),
      "user" -> prop.getProperty("user"),
      "password" -> prop.getProperty("password"),
      "driver" -> prop.getProperty("driver")
    )
    val sqlStr = s"($sqlString) t" // 可以自定义sql语句
    spark.read
      .format("jdbc")
      .options(optionMap)
      .option("dbtable", sqlStr)
      .load()
  }

  //  def readFromMysql(spark: SparkSession, propName: String, tableName: String): DataFrame = {
  //    val prop = PropertiesUtils.loadProperties(propName)
  //    val mysqlUrl = prop.getProperty("url")
  //    spark.read.jdbc(mysqlUrl, tableName, prop)
  //  }
  def writeToMysql(df: DataFrame, propName: String, tableName: String, saveMode: String): Unit = {
    val prop = PropertiesUtils.loadProperties(propName)
    val mysqlUrl = prop.getProperty("url")
    df.write.mode(saveMode).jdbc(mysqlUrl, tableName, prop)
  }
}
