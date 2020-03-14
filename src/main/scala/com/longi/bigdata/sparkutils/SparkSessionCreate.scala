package com.longi.bigdata.sparkutils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Author: whn
 * Date: 2019-8-7 15:51
 * Version: 1.0
 * Function:
 */
object SparkSessionCreate {
  def getSparkSession(conf: SparkConf, runningMode: String, appName: String = this.getClass.getSimpleName): SparkSession = {
    var spark: SparkSession = null
    //    val conf = new SparkConf()
    //      .set("spark.driver.extraJavaOptions", "-Dfile.encoding=utf-8")
    //      .set("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8")
    //      .set("spark.debug.maxToStringFields", "1000")
    //      .set("spark.driver.maxResultSize", "2g")
    //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    if (spark == null) {
      spark = SparkSession.builder()
        .appName(appName)
        .master(runningMode)
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()
    }
    spark
  }
}
