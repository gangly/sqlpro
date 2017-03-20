package com.pingan.pbear.core.executor


import com.pingan.pbear.common.SubTask
import com.pingan.pbear.core.datasources.DataSource
import org.apache.spark.sql.hive.HiveContext

import scala.reflect.runtime._


/**
  * Created by zhangrunqin on 16-12-20.

  */

class BatchExecutor(hiveCtx: HiveContext, subTasks: Array[SubTask]) extends BaseExecutor(hiveCtx, subTasks) {
  override def execute():Int = {

    subTasks.sortBy(_.index).foreach(task => {
      val mainTag = task.mainTag
      val subTag = task.subTag
      val option = task.options
      mainTag match {
        case "create" =>
          subTag match {
            case "table" => getDataSourceClass(option).readFrom(hiveCtx, task.options, dfRow)
            case "function" => runSqlDirect(option)
          }
        case "insert" => getDataSourceClass(option).saveTo(hiveCtx, task.options, dfRow)
        case "select" =>
          subTag match {
            case "table" => runSelectCmd(option)
            case "query" => runSqlDirect(option)
          }
        case "use" => runSqlDirect(option)
        case "add" =>
          var sql = option("sql")
          if (hiveCtx.sparkContext.getConf.get("mode") == "debug") {
            sql = s"add jar file:///Users/lovelife/udfs-1.0-SNAPSHOT.jar"
          }
          hiveCtx.sql(sql)
        case _ => ()
      }
    })
    0
  }

  def getDataSourceClass(option: Map[String, String]): DataSource = {
    val dbtype = option("dbtype")
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val cmdClassName = "com.pingan.pbear.core.datasources." + dbtype.toLowerCase.capitalize + "DataSource"
    val dsObj = runtimeMirror.reflectModule(runtimeMirror.staticModule(cmdClassName))
    dsObj.instance.asInstanceOf[DataSource]
  }
}