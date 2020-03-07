package com.longi.bigdata.scalautils

import scala.math.Numeric
import scala.math.Numeric.Implicits._

/**
  * Author: whn
  * Date: 2019-12-6 14:49
  * Version: 1.0
  * Function:  
  */
object GenericStatsUtils {
  def main(args: Array[String]): Unit = {
    val arr = Array(1, 2, 3, 4, 5)
    println(mean(arr))
    println(variance(arr))
    println(stdDev(arr))
    println(median(arr))
  }

  // 均值
  def mean[T: Numeric](xs: Iterable[T]): Double = xs.sum.toDouble / xs.size

  // 方差
  def variance[T: Numeric](xs: Iterable[T]): Double = {
    val avg = mean(xs)
    xs.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / xs.size
  }

  // 标准差
  def stdDev[T: Numeric](xs: Iterable[T]): Double = math.sqrt(variance(xs))

  // 中位数
  def median[T: Numeric](xs: Iterable[T]): Double = {
    val seqSize = xs.size
    val xsSeq = xs.toSeq
    val seqMedian = if (seqSize % 2 == 0) {
      val v1 = seqSize / 2 - 1
      val v2 = v1 + 1
      (xsSeq(v1).toDouble() + xsSeq(v2).toDouble()) / 2
    } else {
      xsSeq(seqSize / 2).toDouble()
    }
    seqMedian
  }
}
