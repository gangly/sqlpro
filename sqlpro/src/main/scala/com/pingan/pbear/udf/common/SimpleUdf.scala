package com.pingan.pbear.udf.common

/**
  * 所有简单udf函数可集中放在此类中
  * Created by lovelife on 17/2/16.
  */
object SimpleUdf {
  def strLen(str: String): Int = {
    str.length
  }
}
