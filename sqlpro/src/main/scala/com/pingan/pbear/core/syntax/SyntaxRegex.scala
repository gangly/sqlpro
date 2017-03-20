package com.pingan.pbear.core.syntax

import com.pingan.pbear.common.SyntaxConf

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

/**
  * Created by lovelife on 17/2/21.
  */
object SyntaxRegex {

  def getUseCmdInfo(cmd: String): Map[String, String] = {
    // 不用取值，正则匹配可用来判断指令格式是否正确
    val regex = getRegex
    val regex() = cmd
    Map("sql" -> cmd)
  }

  def getAddCmdInfo(cmd: String): Map[String, String] = {
    val regex = getRegex
    val regex(filetype, path) = cmd
    Map("type"-> filetype, "path" -> path, "sql" -> cmd)
  }

  def getQueryCmdInfo(cmd: String): Map[String, String] = {
    val regex = getRegex
    val regex() = cmd
    Map("sql" -> cmd)
  }

  def getSetCmdInfo(cmd: String): Map[String, String] = {
    val regex = getRegex
    val regex(key, value) = cmd
    Map("key"-> key, "value"-> value)
  }

  def getSelectCmdInfo(cmd: String): Map[String, String] = {
    val regex = getRegex
    val regex(tmpTable, sql) = cmd
    Map("tmptable"-> tmpTable, "sql"-> sql)
  }

  def getCreateTempFunctionCmdInfo(cmd: String): Map[String, String] = {

    val regex = getRegex
    val regex() = cmd
    Map("sql" -> cmd)
  }

  def getCreateTempTableCmdInfo(cmd: String): Map[String, String] = {
    val regex = getRegex
    val regex(tmpTable, dbType, option, sql) = cmd
    Map("tmptable" -> tmpTable,
      "dbtype" -> dbType,
      "option" -> option,
      "sql" -> sql
    )
  }

  def getInsertTableCmdInfo(cmd: String): Map[String, String] = {
    val regex = getRegex
    val regex(dbType, option, sql) = cmd
    Map("dbtype" -> dbType,
      "option" -> option,
      "sql" -> sql
    )
  }

  def getRegex: Regex = {
    // 获取调用该函数的方法名
    val invokedMethod = Thread.currentThread().getStackTrace()(2).getMethodName()
    val cmd = invokedMethod.substring("get".length, invokedMethod.indexOf("CmdInfo"))
    val syntaxText = SyntaxConf.getSyntaxText(cmd)
    val regexText = parseSyntax(syntaxText)
    new Regex(regexText)
  }

  def apply(syntaxName: String):Regex = {
    val syntaxText = SyntaxConf.getSyntaxText(syntaxName)
    val regexText = parseSyntax(syntaxText)
    try {
      new Regex(regexText)
    } catch {
      case _: Exception => throw new Exception(s"syntax paser error: $regexText")
    }
  }

  private def parseSyntax(syntax: String) = {
    val processedSyntax = syntax.replaceAll("\\t", " ").replaceAll("\\s+", " ")
    val words = processedSyntax.split(" ")

    val regexText = new ListBuffer[String]()
    words.foreach(word => word match {
      case "*" => regexText += ".*"
//      case "()" => regexText += "(\\S+)"
      case tag if tag.startsWith("(") && tag.endsWith(")")  =>
        val tagRegex =
          if (tag.startsWith("(*")) "(.*)"
          else if (tag.startsWith("('")) "[\'\"](\\S*)[\'\"]"
          else if (tag.contains("|")) {
            val content = tag.substring("(".length, tag.length-1)
            s"((?:$content))"
          } else "(\\S+)"
        regexText += tagRegex
      case _ =>
        if (word.contains("|")) {
          regexText += s"(?:$word)"
        } else {
          regexText += word
        }
    })
    // 忽略大小写
    "(?i)"+regexText.toList.mkString(" ")
  }
}
