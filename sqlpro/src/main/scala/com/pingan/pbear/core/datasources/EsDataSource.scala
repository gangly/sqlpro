package com.pingan.pbear.core.datasources

import com.pingan.pbear.util.StringUtil
import org.apache.spark.sql.hive.HiveContext
import org.elasticsearch.spark.sql._

/**
  * Created by lovelife on 17/2/24.
  */
object EsDataSource extends DataSource{

  override
  def readFrom(hiveCtx: HiveContext, info: Map[String, String], dfRow: Int): Unit = {
    val sql = info("sql")
    val option = getMappedOption(info)
    val index = option("index")
    val tmpTable = info("tmptable")
    val table = StringUtil.getTableFromSelectSql(sql)
    hiveCtx.esDF(index).registerTempTable(table)
    runSqlAndShowResult(hiveCtx, sql, dfRow).registerTempTable(tmpTable)
  }

  override
  def saveTo(hiveCtx: HiveContext, info: Map[String, String], dfRow: Int): Unit = {
    val sql = info("sql")
    val option = getMappedOption(info)
    // todo 支持其他格式
//    val format = if(option.contains("format")) option("format") else "json"
    val index = option("index")
    runSqlAndShowResult(hiveCtx, sql, dfRow).saveToEs(index)
  }
}
