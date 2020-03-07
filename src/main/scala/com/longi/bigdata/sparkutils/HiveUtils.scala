package com.longi.bigdata.sparkutils

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Author: whn
  * Date: 2019-9-20 15:39
  * Version: 1.0
  * Function:
  */
object HiveUtils {
  def readFromHive(spark: SparkSession, tableName: String, args: Array[String]): DataFrame = {
    val dataLoadType = args(0) // "1" 是全量加载  "2"是按时间段加载，需要传入起始时间和截止时间
    //('1'):全量加载 ---- ('2','begin','end'):增量加载(需要传入起始和截止日期) ---- (传其他参数):加载程序运行的前一天的数据.
    val plDtlData = if (dataLoadType.equals("1")) {
      println("加载模式: 全量加载...")
      val onceSql = s"select * from $tableName"
//      spark.sql(onceSql).limit(10000)
      spark.sql(onceSql)
    }
    else {
      val preDay = args(1)
      val now = args(2)
      println("加载模式: 增量加载...")
      println(s"[起始日期:$preDay, 截止日期:$now)")
      val daySql = s"select * from $tableName where dt>'$preDay' and dt<'$now'"
      spark.sql(daySql)
    }
    plDtlData
  }

  def writeToHiveByPartition(df: DataFrame, spark: SparkSession, tableName: String, partitionName: String, dataLoadMode: String): Unit = {
    //    spark.sql("set hive.exec.dynamic.partition.mode = nonstrict") // 动态写入分区需要设置这两个属性
    //    spark.sql("set hive.exec.dynamic.partition = true")
    if (dataLoadMode.equals("1")) {
      df.write.partitionBy(partitionName)
        .format("hive")
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
    else {
      df.write.partitionBy(partitionName)
        .format("hive")
        .mode(SaveMode.Append)
        .saveAsTable(tableName)
    }

  }

  def writeToHive(df: DataFrame, spark: SparkSession, tableName: String, saveMode: String): Unit = {
    df.write
      .format("hive")
      .mode(saveMode)
      .saveAsTable(tableName)
  }

}
