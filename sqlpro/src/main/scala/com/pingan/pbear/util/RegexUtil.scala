package com.pingan.pbear.util

import com.pingan.pbear.common.NotFindPbearException

/**
  * Created by lovelife on 17/3/2.
  */
object RegexUtil {

   def getSetCmdValue(key: String, text: String): String = {
     val regex = s".*set\\s+$key\\s+=\\s+(\\S+);.*".r
     try {
       val regex(value) = text
       value
     } catch {
       case _: Exception => throw new NotFindPbearException(s"没有找到${key}配置, 必须在任务脚本中设置")
     }
  }
}
