package com.pingan.pbear.core.datasources

import com.pingan.pbear.util.KafkaSink
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by lovelife on 17/3/1.
  */
object KafkaDataSource extends DataSource{
  override def readFrom(hiveCtx: HiveContext, info: Map[String, String], dfRow: Int): Unit = {
    val sql = info("sql")
    val tmpTable = info("tmptable")
    runSqlAndShowResult(hiveCtx, sql, dfRow).registerTempTable(tmpTable)
  }

  override def saveTo(hiveCtx: HiveContext, info: Map[String, String], dfRow: Int): Unit = {
    val sql = info("sql")
    val option = getMappedOption(info)
    val broker = option("broker")
    val topic = option("topic")

    val kafkaSink = KafkaSink(broker)
    val df = runSqlAndShowResult(hiveCtx, sql, dfRow)
    df.toJSON.foreach(line => {
      kafkaSink.send(topic, line)
    })
  }
}
