package com.pingan.pbear.core.datasources

import com.pingan.pbear.util.StringUtil
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by lovelife on 17/2/24.
  */
object HdfsDataSource extends DataSource{

  override
  def readFrom(hiveCtx: HiveContext, info: Map[String, String], dfRow: Int): Unit = {
    val sql = info("sql")
    val tmpTable = info("tmptable")
    val table = {
      val aTable = StringUtil.getTableFromSelectSql(sql)
      if (aTable == tmpTable) aTable+StringUtil.getCurrentTime else aTable
    }

    val mappedOption = getMappedOption(info)
    val path = mappedOption("path")
    val format = mappedOption("format")
    format match {
      case "json" => hiveCtx.read.json(path).registerTempTable(table)
      case "parquet" => hiveCtx.read.parquet(path).registerTempTable(table)
      case _ => hiveCtx.read.load(path).registerTempTable(table)
    }
    runSqlAndShowResult(hiveCtx, sql, dfRow).registerTempTable(tmpTable)
  }

  override
  def saveTo(hiveCtx: HiveContext, info: Map[String, String], dfRow: Int): Unit = {
    val sql = info("sql")
    // Accepted modes are 'overwrite', 'append', 'ignore', 'error'.
    val mode = getInsertMode(info)
    val mappedOption = getMappedOption(info)
    val path = mappedOption("path")
    val format = mappedOption("format")
    val dataFrame = runSqlAndShowResult(hiveCtx, sql, dfRow)
    format match {
      case "json" => dataFrame.write.mode(mode).json(path)
      case "parquet" => dataFrame.write.mode(mode).parquet(path)
      case _ => dataFrame.write.mode(mode).save(path)
    }
  }

}
