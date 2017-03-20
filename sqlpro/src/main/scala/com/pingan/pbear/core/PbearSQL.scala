package com.pingan.pbear.core

import com.pingan.pbear.common.{AppConf, NotFindPbearException, SubTask}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import com.pingan.pbear.util.FuncsUtil.readHdfsFile
import com.pingan.pbear.core.parser._
import com.pingan.pbear.core.executor._
import com.pingan.pbear.util.{HDFSTool, RegexUtil, StringUtil}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe
import scala.collection.JavaConversions._
import scala.io.Source
/**
  * Created by zhangrunqin on 16-12-20.
  * 注意：
  * 1.在开发环境下,请将你开发环境的hive-site.xml和hdfs-site.xml文件放入resource目录下.
  * 2.修改package com.pingan.pbear.util.HDFSTool中的configBasePath为你开发环境的hadoop配置文件地址.
  */
object PbearSQL extends Logging{

  def main(args: Array[String]) {
    val usage =
      """
      Usage: spark-submit --master [local/yarn-cluster] --class com.pingan.pbear.core.PbearSQL pbearsql-1.0-**-SNAPSHOT-jar-with-dependencies.jar taskfile
      taskfile: 任务脚本名,必须指定
      """
    if (args.length < 1) {
      log.error(s"Args Error: $usage")
    }

    log.info("\n******PbearSQL Service start!******\n")
    val envMode = AppConf.getEnvMode
    log.info(s" ****** running in $envMode mode ******\n")

    try {
      val taskConfigFile = args(0)
      val configContent = readTaskFile(taskConfigFile, envMode)
      val cleanedConfigContent = cleanConfigContent(configContent)
      val appName = RegexUtil.getSetCmdValue("spark.app.name", cleanedConfigContent.toLowerCase)
      val taskType = RegexUtil.getSetCmdValue("pbear.task.type", cleanedConfigContent.toLowerCase)

      val subTasks = parseJob(cleanedConfigContent)
      assert(!subTasks.isEmpty, "没有找到一个任务，退出程序")
      log.info(s"\\n>>>jobType = $taskType")
      subTasks.foreach(task => log.info(s"\n>>>$task"))

      val sparkConf: SparkConf = createAndSetSparkConf(envMode, subTasks)
      val sc = new SparkContext(sparkConf)
      val hiveCtx = new HiveContext(sc)
      taskType match {
        case "export" => new ExportExecutor(hiveCtx, subTasks).execute()
        case "batch" => new BatchExecutor(hiveCtx, subTasks).execute()
        case "streaming" => new StreamingExecutor(hiveCtx, subTasks).execute()
        case _ => throw new Exception(s"unsupported job type.{$taskType}")
      }
      log.info("\n******SQL Service exit!******\n")
      sc.stop()
    } catch {
      case e: Exception =>
        log.error(e.getMessage)
        e.printStackTrace()
    }
  }

  private def cleanConfigContent(jobConfig: String): String = {
    jobConfig.split("\n").filter(line => !line.trim.startsWith("--"))
      .mkString(" ")
      .replace("\t", " ")
      .replaceAll("\\s+", " ")
  }

  def parseJob(jobConfig: String): Array[SubTask] ={
    val subTasks = new ArrayBuffer[SubTask]()
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    // 每条指令已经trimed
    val statements = jobConfig.split(";").map(_.trim)

    var index = 0
    statements.filter(s => s.length > 3).foreach(instruction => {
      var cmdType = instruction.split(" ")(0).trim
      //"T1=select a from b" statement is special
      if(instruction.contains("=") && instruction.substring(0, instruction.indexOf("=")).trim.split(" ").length == 1){
        cmdType = instruction.substring(instruction.indexOf("=") + 1).trim.split(" ")(0).trim
      }
      val cmdClassName = "com.pingan.pbear.core.parser." + cmdType.toLowerCase.capitalize + "Instruction"
      val obj = runtimeMirror.reflectModule(runtimeMirror.staticModule(cmdClassName))
      subTasks ++= obj.instance.asInstanceOf[BaseInstruction].parse(Map("instruction" -> instruction, "index" -> index))
      index += 1
    })
    subTasks.toArray
  }

  private def createAndSetSparkConf(envMode: String, subTasks: Array[SubTask]): SparkConf = {
    val conf = new SparkConf()
    // 设置sparkconf参数，可在excuter中获取
    conf.set("mode", envMode)
    // 本地调试运行时设置为local，在yarn-cluster上运行时请选择其他模式
    if (conf.get("mode") == "debug") {
      conf.setMaster("local[2]")
    }
    conf.set("spark.sql.hive.thriftServer.singleSession", "true")

    //设置应用运行参数
    log.info("\n********设置应用参数********\n")
    subTasks.filter(t => t.mainTag == "set" && t.subTag == "spark")
      .foreach(f =>{
        conf.set(f.options("key"), f.options("value"))
      })

    // 若是es数据库类型，需要全局设置sparkconf
    subTasks.filter(t => t.mainTag == "create" && t.subTag == "es")
      .foreach(f => {
        val option = f.options("option")
        val mappedOption = StringUtil.parseKeyValueWithQuotes(option)
        if (mappedOption.contains("nodes")) conf.set("es.nodes", mappedOption("nodes"))
        if (mappedOption.contains("port")) conf.set("es.port", mappedOption("port"))
      })
    conf
  }

  private def readTaskFile(filePath: String, runMode: String): String = {
    if (List("debug").contains(runMode)) {
      Source.fromURL(getClass.getResource("/tasksql/" + filePath), "UTF-8").mkString
    } else {
      val fullPath = if (filePath.startsWith("hdfs://")) filePath else HDFSTool.getHdfsRoot()+AppConf.getTasksHdfsPath+filePath
      log.info(s"#### config path $fullPath ######\n")
      readHdfsFile(fullPath)
    }
  }
}