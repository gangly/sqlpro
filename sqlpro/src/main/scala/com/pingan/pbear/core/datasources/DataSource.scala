package com.pingan.pbear.core.datasources

import com.pingan.pbear.core.common.SparkSqlTool
import com.pingan.pbear.util.StringUtil
import org.apache.spark.sql.hive.HiveContext
/**
  * Created by lovelife on 17/2/23.
  */
trait DataSource extends SparkSqlTool{

  def getInsertMode(info: Map[String, String]): String = {
    val mappedOption = getMappedOption(info)
    val mode = if (mappedOption.contains("mode")) mappedOption("mode") else "append"
    mode
  }

  def getMappedOption(info: Map[String, String]): Map[String, String] = {
    val option = info("option")
    StringUtil.parseKeyValueWithQuotes(option)
  }


  def getRenamedTable(tmpTable: String, sql: String): String = {
    val table = StringUtil.getTableFromSelectSql(sql)
    table
  }

  def readFrom(hiveCtx : HiveContext, info: Map[String, String], dfRow: Int)
  def saveTo(hiveCtx : HiveContext, info: Map[String, String], dfRow: Int)
}
