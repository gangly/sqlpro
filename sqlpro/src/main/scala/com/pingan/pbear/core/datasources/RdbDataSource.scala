package com.pingan.pbear.core.datasources

import java.util.Properties

import com.pingan.pbear.util.{FuncsUtil, StringUtil}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by lovelife on 17/2/23.
  */
object RdbDataSource extends DataSource{

  override
  def readFrom(hiveCtx: HiveContext, info: Map[String, String], dfRow: Int): Unit = {
//    val rdbConf = getRdbConf(info)
    val tmpTable = info("tmptable")
    val sql = info("sql")
    val table = StringUtil.getTableFromSelectSql(sql)
//    val url = rdbConf("url")
//    val prop = createDbProperties(rdbConf("user"), rdbConf("password"))
//    hiveCtx.read.jdbc(url, table, prop).registerTempTable(table)
    queryToDf(hiveCtx, info).registerTempTable(table)
    runSqlAndShowResult(hiveCtx, sql, dfRow).registerTempTable(tmpTable)
  }

   def queryToDf(hiveCtx: HiveContext, info: Map[String, String]): DataFrame = {
    val rdbConf = getRdbConf(info)
    val sql = info("sql")
     log.info("******:"+sql)
    val table = if (info.contains("rdbtable")) info("rdbtable") else StringUtil.getTableFromSelectSql(sql)
    val url = rdbConf("url")
    val prop = createDbProperties(rdbConf("user"), rdbConf("password"))
    hiveCtx.read.jdbc(url, table, prop)
  }

  override
  def saveTo(hiveCtx: HiveContext, info: Map[String, String], dfRow: Int): Unit = {
    val rdbConf = getRdbConf(info)
    val sql = info("sql")
    val option = getMappedOption(info)
    val mode = getInsertMode(info)
    val url = rdbConf("url")
    val table = option("table")
    val prop = createDbProperties(rdbConf("user"), rdbConf("password"))
    runSqlAndShowResult(hiveCtx, sql, dfRow).write.mode(mode).jdbc(url, table, prop)
  }

  def createDbProperties(user: String, password: String):Properties = {
    val properties = new Properties()
    properties.put("user", user)
    properties.put("password", password)
    properties
  }

  def getRdbConf(info: Map[String, String]): Map[String, String] = {
    val option = getMappedOption(info)
    val rdbConf = {
      if(option.contains("key")) FuncsUtil.connStrToConnParams(option("key"))
      else if(option.contains("url")) option
      else throw new Exception("Wrong create instruction option: " + info("option"))
    }
    rdbConf
  }
}
