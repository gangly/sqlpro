package com.pingan.pbear.common

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by lovelife on 17/2/21.
  */
object SyntaxConf {
  val conf = ConfigFactory.load("syntax.conf")

  def getSyntaxText(name: String): String = {
    try {
      conf.getString(name)
    } catch {
      case _: Exception => throw new NotFindPbearException(s"not find such command template: $name")
    }
  }
}
