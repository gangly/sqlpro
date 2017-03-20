package com.pingan.pbear.core.executor

import com.pingan.pbear.common.SubTask
import com.pingan.pbear.core.datasources.RdbDataSource
import com.pingan.pbear.util.StringUtil
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhangrunqin on 16-12-20.
  * 导入导出有一些特殊的地方：需要分批执行,避免内存爆表.
  * 目前只支持从关系型数据库到其他数据源
  *
  * update:2016-12-22
  * 由于Spark SQL不支持offset,一种walk around方案如下：
  * 利用limit,offset分段从rdb中导出数据，经过中间转换，存储到其他数据源
  */

class ExportExecutor(hiveCtx: HiveContext, subTasks: Array[SubTask]) extends BaseExecutor(hiveCtx, subTasks) {

  override def execute():Int = {
    val insertTask = subTasks.filter(task => task.mainTag == "create" && task.subTag == "table").head
    val otherTasks = subTasks.filter(task => task.mainTag != "create" && task.subTag != "table")
    val batchSize = getBatchSize
    log.info(s"the batch size of export task is: $batchSize")

    val num = getTotalRecordsNum(insertTask)
    log.info(s"the total records is: $num")
    (0 to num/batchSize).foreach(i => {
      // 构造新的任务列表，直接调用批处理 excuetor
      val newInsertTask = rebuildInsertSubtask(insertTask, LimitAndOffset(batchSize, i*batchSize))
      var newSubTasks = Array(newInsertTask)
      newSubTasks ++= otherTasks

      new BatchExecutor(hiveCtx, newSubTasks).execute()
    })
    0
  }

  case class LimitAndOffset(limit: Int, offset: Int)

  private def rebuildInsertSubtask(insertTask: SubTask, limitAndOffset: LimitAndOffset): SubTask = {

    val option = insertTask.options
    val sql = option("sql")
    log.info(sql)
    val limit = limitAndOffset.limit
    val offset = limitAndOffset.offset

    val table = StringUtil.getTableFromSelectSql(sql)
    val rdbtable = s"($sql limit $limit offset $offset) as $table"
    log.info("rdbtable:::::"+rdbtable)
    var newOption = Map[String, String]()
    newOption ++= option
    newOption ++= Map("rdbtable" -> rdbtable)
    SubTask("create", "table", newOption, insertTask.index)
  }

  private def getTotalRecordsNum(insertTask: SubTask): Int = {
    val option = insertTask.options
    val sql = option("sql")
    val newSql = "select count(1) as num " + sql.substring(sql.toLowerCase.indexOf(" from "))
    log.info(newSql)
    var newOption = Map[String, String]()
    newOption ++= option
    newOption ++= Map("sql" -> newSql)
    val rdbtable = s"($newSql) as tmptable"
    log.info("*****"+rdbtable)
    newOption ++= Map("rdbtable" -> rdbtable)

    val df = RdbDataSource.queryToDf(hiveCtx, newOption)
    df.head().get(0).toString.toInt
  }

  private def getBatchSize = {
    try {
      val batchSize = mappedSetKv("pbear.batch_size").toInt
      if (batchSize > 10000 && batchSize < 3000000) batchSize else 1000000
    } catch {
      case _: Exception => 1000000
    }
  }
}
