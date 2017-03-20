package com.pingan.pbear.core.parser

import java.util

import com.pingan.pbear.common.{SubTask, WrongCmdPbearException}
import com.pingan.pbear.core.syntax.SyntaxRegex
import org.apache.spark.Logging

import scala.collection.JavaConversions._

/**
  * Created by zhangrunqin on 16-12-19.
  */
object AddInstruction extends BaseInstruction with Logging{
  override def parse(params: Map[String, Any]): util.List[SubTask] = {
    val subTasks = new util.ArrayList[SubTask]()
    val instruction = params.get("instruction").get.toString
    val index = params.get("index").get.toString.toInt

    try {
      val option = SyntaxRegex.getAddCmdInfo(instruction)
      subTasks += SubTask("add", option("type"), option, index)
    } catch {
      case _: Exception => throw new WrongCmdPbearException(s"Wrong add command: $instruction")
    }
    subTasks
  }
}
