package com.pingan.pbear.core.parser

import java.util

import com.pingan.pbear.common.{SubTask, WrongCmdPbearException}
import com.pingan.pbear.core.syntax.SyntaxRegex
import org.apache.spark.Logging

import scala.collection.JavaConversions._
/**
  * Created by zhangrunqin on 16-12-20.
  */
object SelectInstruction extends BaseInstruction with Logging{
  override def parse(params: Map[String, Any]): util.List[SubTask] = {

    val subTasks = new util.ArrayList[SubTask]()
    val instruction = params.get("instruction").get.toString
    val index = params.get("index").get.toString.toInt
    try {
        val option = SyntaxRegex.getSelectCmdInfo(instruction)
        subTasks += SubTask("select", "table", option, index)
    } catch {
      case _: Exception =>
        try{
          val option = SyntaxRegex.getQueryCmdInfo(instruction)
          subTasks += SubTask("select", "query", option, index)
        } catch {
          case _: Exception => throw new WrongCmdPbearException(s"Wrong select command: $instruction")
        }
    }
    subTasks
  }

}
