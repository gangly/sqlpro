package com.pingan.pbear.core.executor

import com.pingan.pbear.common.{AppConf, SubTask}
import com.pingan.pbear.core.common.SparkSqlTool
import com.pingan.pbear.udf.RegisterUdf
import org.apache.spark.sql.hive.HiveContext

import scala.util.Try

/**
  * Created by zhangrunqin on 16-12-20.
  * Modified by ligang on 17-2-12
  */
abstract class BaseExecutor(hiveCtx: HiveContext, subTasks: Array[SubTask]) extends Serializable with SparkSqlTool {

  // set指令中kv值，可在基类中直接使用
  val mappedSetKv = {
    var theSetInstructions: Map[String, String] = Map()
    subTasks.filter(f => f.mainTag == "set").foreach(f => theSetInstructions ++= Map(f.options("key")->f.options("value")))
    theSetInstructions
  }
  val dfRow = Try(mappedSetKv("df.show.rows").toString.toInt)
    .getOrElse(AppConf.getShowDataFrameRows)

  initExecutor()

  def initHiveCtx() = {
    RegisterUdf.registerFunctions(hiveCtx)
  }


  private def initExecutor() = {
    initHiveCtx()
  }

  protected def runSqlDirect(option: Map[String, String]) = {
    val sql = option("sql")
    runSqlAndShowResult(hiveCtx, sql, dfRow)
  }

  protected def runSelectCmd(option: Map[String, String]) = {
    val tmpTable = option("tmptable")
    val sql = option("sql")
    runSqlAndShowResult(hiveCtx, sql, dfRow).registerTempTable(tmpTable)
  }

  def execute(): Any


//  def saveToES(selectTask: SubTask, insertTask: SubTask) = {
//    log.info("\nsaveToES logic.\n")
//    val query = selectTask.options.getOrElse("sql", "").toString.trim
//    val format = selectTask.options.getOrElse("format", "json").toString.trim.toLowerCase
//    val path = insertTask.options.getOrElse("dstPath","").toString.trim
//
//    val df = hiveCtx.sql(query)
//    //TODO 1.支持自定义元数据 2.支持自定义id
//    format match {
//      case "json" =>
//        df.toJSON.saveJsonToEs(path)
//      case "map" =>
//        val schema = df.schema
//        val len = schema.fields.length
//        //Map的类型固定成了String -> String
//        df.map(r => {
//          var m = Map[String, String]()
//          (0 until len).foreach(i => {
//            m += (schema.fields(i).name -> r.getAs[String](i))
//          })
//          m
//        }).saveToEs(path)
//    }
//    checkAndShowDF(df, query)
//  }

}
