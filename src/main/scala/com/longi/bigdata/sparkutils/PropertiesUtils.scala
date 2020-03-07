package com.longi.bigdata.sparkutils

import java.util.Properties

/**
  * Author: whn
  * Date: 2020-1-20 14:33
  * Version: 1.0
  * Function:  通过自定义的properties名称，获取相应的properties，主要包括测试和生产环境的DBMS信息
  */
object PropertiesUtils {
  def loadProperties(proName: String): Properties = {
    val properties = new Properties()
    val returnProp = new Properties()
    val path = Thread.currentThread().getContextClassLoader.getResourceAsStream("bigdata.properties") //文件要放到resource文件夹下
    properties.load(path)

    proName match {
      case "ora.test" =>
        returnProp.setProperty("driver", properties.getProperty("ora.test.driver"))
        returnProp.setProperty("url", properties.getProperty("ora.test.url"))
        returnProp.setProperty("user", properties.getProperty("ora.test.user"))
        returnProp.setProperty("password", properties.getProperty("ora.test.password"))
      case "ora.product.dim" =>
        returnProp.setProperty("driver", properties.getProperty("ora.product.driver"))
        returnProp.setProperty("url", properties.getProperty("ora.product.url"))
        returnProp.setProperty("user", properties.getProperty("ora.product.dim.user"))
        returnProp.setProperty("password", properties.getProperty("ora.product.dim.password"))
      case "ora.product.ods" =>
        returnProp.setProperty("driver", properties.getProperty("ora.product.driver"))
        returnProp.setProperty("url", properties.getProperty("ora.product.url"))
        returnProp.setProperty("user", properties.getProperty("ora.product.ods.user"))
        returnProp.setProperty("password", properties.getProperty("ora.product.ods.password"))
      case "ora.product.dw" =>
        returnProp.setProperty("driver", properties.getProperty("ora.product.driver"))
        returnProp.setProperty("url", properties.getProperty("ora.product.url"))
        returnProp.setProperty("user", properties.getProperty("ora.product.dw.user"))
        returnProp.setProperty("password", properties.getProperty("ora.product.dw.password"))
      case "ora.product.temp" =>
        returnProp.setProperty("driver", properties.getProperty("ora.product.driver"))
        returnProp.setProperty("url", properties.getProperty("ora.product.url"))
        returnProp.setProperty("user", properties.getProperty("ora.product.temp.user"))
        returnProp.setProperty("password", properties.getProperty("ora.product.temp.password"))
      case "ora.product.app" =>
        returnProp.setProperty("driver", properties.getProperty("ora.product.driver"))
        returnProp.setProperty("url", properties.getProperty("ora.product.url"))
        returnProp.setProperty("user", properties.getProperty("ora.product.app.user"))
        returnProp.setProperty("password", properties.getProperty("ora.product.app.password"))
      case "mysql.test" =>
        returnProp.setProperty("driver", properties.getProperty("mysql.test.driver"))
        returnProp.setProperty("url", properties.getProperty("mysql.test.url"))
        returnProp.setProperty("user", properties.getProperty("mysql.test.user"))
        returnProp.setProperty("password", properties.getProperty("mysql.test.password"))
      case "mysql.product" =>
        returnProp.setProperty("driver", properties.getProperty("mysql.product.driver"))
        returnProp.setProperty("url", properties.getProperty("mysql.product.url"))
        returnProp.setProperty("user", properties.getProperty("mysql.product.user"))
        returnProp.setProperty("password", properties.getProperty("mysql.product.password"))
      case "hw.au" =>
        returnProp.setProperty("ak", properties.getProperty("hw.au.ak"))
        returnProp.setProperty("sk", properties.getProperty("hw.au.sk"))
    }
    returnProp
  }
}

