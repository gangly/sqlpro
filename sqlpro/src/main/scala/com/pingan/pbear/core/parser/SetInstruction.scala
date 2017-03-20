package com.pingan.pbear.core.parser

import java.util

import com.pingan.pbear.common.{SubTask, WrongCmdPbearException}
import com.pingan.pbear.core.syntax.SyntaxRegex
import org.apache.spark.Logging

import scala.collection.JavaConversions._

/**
  * Created by zhangrunqin on 16-12-19.
  */
object SetInstruction extends BaseInstruction with Logging {
  override def parse(params: Map[String, Any]): util.List[SubTask] = {

    val subTasks = new util.ArrayList[SubTask]()
    val instruction = params("instruction").toString
    val index = params("index").toString.toInt
    try {
      val option = SyntaxRegex.getSetCmdInfo(instruction)
      val key = option("key")
      val subTag = key.split('.')(0).toLowerCase
      subTasks += SubTask("set", subTag, option, index)
    } catch {
      case e: Exception => throw new WrongCmdPbearException(s"Wrong set command: $instruction")
    }
    subTasks
  }
}
