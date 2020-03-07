package com.longi.bigdata

import com.longi.bigdata.sparkutils.MysqlUtils
import org.apache.spark.sql.SparkSession

/**
  * Author: whn
  * Date: 2020-1-20 14:47
  * Version: 1.0
  * Function:
  */
object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Test").master("local").getOrCreate()
//    val prop = PropertiesUtils.loadProperties("mysql.product")

    val sql = "SELECT version_day FROM app_pl_predict_res limit 100"
    val df = MysqlUtils.readFromMysql(spark,"mysql.product",sql)
    df.show()
  }
}
