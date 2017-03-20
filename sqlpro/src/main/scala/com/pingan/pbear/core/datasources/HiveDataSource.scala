package com.pingan.pbear.core.datasources

import org.apache.spark.sql.hive.HiveContext

/**
  * Created by lovelife on 17/2/24.
  */
object HiveDataSource extends DataSource {

  override def readFrom(hiveCtx: HiveContext, info: Map[String, String], dfRow: Int): Unit = {
    val tmpTable = info("tmptable")
    val sql = info("sql")
    runSqlAndShowResult(hiveCtx, sql, dfRow).registerTempTable(tmpTable)
  }

  override def saveTo(hiveCtx: HiveContext, info: Map[String, String], dfRow: Int): Unit = {
    // hql only support into, overwrite
    val mode = {
      val tmpMode = getInsertMode(info)
      if (tmpMode == "append") "into" else tmpMode
    }
    val mappedOption = getMappedOption(info)
    val table = mappedOption("table")
    val tmpSql = info("sql")
    val sql = s"insert $mode table $table $tmpSql"
    runSqlAndShowResult(hiveCtx, sql, dfRow)
  }
}
