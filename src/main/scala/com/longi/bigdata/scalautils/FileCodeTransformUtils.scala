package com.longi.bigdata.scalautils

import java.io.File

import org.apache.commons.io.FileUtils


/**
  * Author: whn
  * Date: 2019-7-9 16:45
  * Version: 1.0
  * Function:  文件编码转换工具
  */
object FileCodeTransformUtils extends App {

  /**
    *
    * @param filePath 源文件路径，转码后目标文件路径不变
    * @param sourceCode 源文件的编码
    * @param targetCode 目标文件的编码
    * @example
    *          fileCodeTransform("F:\\test.csv", "GB2312", "UTF8")
    *          即把该路径下test.csv的编码格式由GB2312转换为UTF8
    * @return 无返回值
    */
  def fileCodeTransform(filePath: String, sourceCode: String, targetCode: String): Unit = {
    val fileCodeTransformed = new File(filePath) // 基于源文件转码，目标文件地址不变
    val content = FileUtils.readFileToString(fileCodeTransformed, sourceCode)
    FileUtils.write(fileCodeTransformed, content, targetCode)
    
  }
}
