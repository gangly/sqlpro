package com.pingan.pbear.util

import java.net.URL

/**
  * Created by lovelife on 17/2/21.
  */
object StringUtil {
  /**
    * "file:/home/whf/cn/fh" -> "/home/whf/cn/fh"
    * "jar:file:/home/whf/foo.jar!cn/fh" -> "/home/whf/foo.jar"
    */
  def getRootPath(url: URL): String = {
    val fileUrl = url.getFile()
    val pos = fileUrl.indexOf('!')

    if (-1 == pos) {
      return fileUrl
    }
    fileUrl.substring(5, pos)
  }

  /**
    * "cn.fh.lightning" -> "cn/fh/lightning"
    *
    * @param fullClassName 全路径类名
    * @return 转换后的路径名
    */
  def dotToSplash(fullClassName: String): String = {
    fullClassName.replaceAll("\\.", "/")
  }

  /**
    * "cn.fh.lightning" -> "cn/fh/lightning"
    *
    * @param fullClassName 全路径类名
    * @return 转换后的路径名
    */
  def splashToDot(fullClassName: String): String = {
    fullClassName.replaceAll("/", "\\.")
  }

  /**
    * "Apple.class" -> "Apple"
    */
  def trimExtension(name: String): String = {
    val pos = name.indexOf('.')
    if (-1 != pos) {
      return name.substring(0, pos)
    }
    name
  }

  /**
    * /application/home -> /home
    *
    * @param uri 路径名
    * @return 处理后的路径名
    */
  def trimURI(uri: String): String = {
    val trimmed = uri.substring(1)
    val splashIndex = trimmed.indexOf('/')

    trimmed.substring(splashIndex)
  }

  def trimQuotes(text: String): String = {
    text.replace("\"", " ").replace("\'", " ").trim
  }

  private def isSurroundedWithTag(text: String, tag: String): Boolean = {
    text.startsWith(tag) && text.endsWith(tag)
  }

  private def isSurroundedWithQuotes(text: String): Boolean = {
    isSurroundedWithTag(text, "\"") || isSurroundedWithTag(text, "\'")
  }

  def parseKeyValueWithQuotes(text: String): Map[String, String] = {
      val content =
        if (isSurroundedWithQuotes(text))
          text.substring(1, text.length - 1)
        else text
      parseKeyValue(content)
  }

  def parseKeyValue(text: String): Map[String, String] = {
    var kvMap = Map[String, String]()
    text.split("\\|").foreach(words => {
      val kv = words.split("=")
      kvMap ++= Map(kv(0) -> kv(1))
    })
    kvMap
  }

  def getCurrentTime = {
    System.currentTimeMillis().toString
  }

  def getDbFromJdbcUrl(url: String): String = {
    val words = url.split("/")
    words(words.length-1)
  }

  def getTableFromSelectSql(sql: String): String = {
    sql.toLowerCase.split("from")(1).trim.split(" ")(0)
  }
}
