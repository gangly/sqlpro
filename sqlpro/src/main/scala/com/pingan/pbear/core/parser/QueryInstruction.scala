package com.pingan.pbear.core.parser

import java.util
import com.pingan.pbear.common.{SubTask, WrongCmdPbearException}
import com.pingan.pbear.core.syntax.SyntaxRegex
import scala.collection.JavaConversions._
/**
  * Created by lovelife on 17/2/28.
  */
object QueryInstruction extends BaseInstruction{
  override def parse(params: Map[String, Any]): util.List[SubTask] = {
    val subTasks = new util.ArrayList[SubTask]()
    val instruction = params.get("instruction").get.toString
    val index = params.get("index").get.toString.toInt

    try {
      val info = SyntaxRegex.getQueryCmdInfo(instruction)
      subTasks += SubTask("query", "query", info, index)
      subTasks
    } catch {
      case _:Exception => throw new WrongCmdPbearException(s"Wrong query command: $instruction")
    }
  }
}
