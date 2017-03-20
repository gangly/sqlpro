package com.pingan.pbear.core.parser

import java.util
import com.pingan.pbear.common.SubTask

/**
  * Created by zhangrunqin on 16-12-19.
  */
trait BaseInstruction {
  def parse(params:Map[String, Any]):util.List[SubTask]
}
