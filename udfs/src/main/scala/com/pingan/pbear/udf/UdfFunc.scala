package com.pingan.pbear.udf

/**
  * Created by lovelife on 16/12/30.
  *
  * UDF函数需要继承hive UDF类，必须实现evaluate函数
  * 注意不能写成def evaluate(s: String): Int = s.length，会导致找不到类
  * 由sacla与java类型兼容引起
  *
  */

import org.apache.hadoop.hive.ql.exec.UDF

class StrLen extends UDF {
  def evaluate(str: String): Int = {
    str.length()
  }
}


