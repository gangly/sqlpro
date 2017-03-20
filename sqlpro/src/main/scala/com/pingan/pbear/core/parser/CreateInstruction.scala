package com.pingan.pbear.core.parser

import java.util

import com.pingan.pbear.common.{SubTask, WrongCmdPbearException}
import com.pingan.pbear.core.syntax.SyntaxRegex
import com.pingan.pbear.util.FuncsUtil.connStrToConnParams
import org.apache.spark.Logging

import scala.collection.JavaConversions._
/**
  * Created by zhangrunqin on 16-12-20.
  * TODO:增加建hive表语句
  */
object CreateInstruction extends BaseInstruction with Logging {
  override def parse(params: Map[String, Any]): util.List[SubTask] = {
    val subTasks = new util.ArrayList[SubTask]()
    val instruction = params("instruction").toString
    val index = params("index").toString.toInt

    try {
      subTasks += parseTempTable(instruction, index)
    } catch {
      case _:Exception =>
        subTasks += parseTempFunction(instruction, index)
    }
    subTasks
  }

  def parseTempTable(instruction: String, index: Int): SubTask = {
    try {
      val info = SyntaxRegex.getCreateTempTableCmdInfo(instruction)
      SubTask("create", "table", info, index)
    } catch {
      case _:Exception => throw new WrongCmdPbearException(s"Wrong create command: $instruction")
    }
  }

  def parseTempFunction(instruction: String, index: Int): SubTask = {
    try {
      val info = SyntaxRegex.getCreateTempFunctionCmdInfo(instruction)
      SubTask("create", "function", info, index)
    } catch {
      case _:Exception => throw new WrongCmdPbearException(s"Wrong create command: $instruction")
    }
  }
}
