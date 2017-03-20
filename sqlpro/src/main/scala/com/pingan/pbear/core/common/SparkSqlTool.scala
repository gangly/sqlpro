package com.pingan.pbear.core.common

import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by lovelife on 17/2/28.
  */
trait SparkSqlTool extends Logging{
  def runSqlAndShowResult(hiveCtx: HiveContext, sql: String, showDfRows: Int): DataFrame = {
    val df = hiveCtx.sql(sql)
    if (showDfRows > 0) {
      df.show(showDfRows)
      log.info(s"show dataframe ok: $sql")
    }
    df
  }
}
