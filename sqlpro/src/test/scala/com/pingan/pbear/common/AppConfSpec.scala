package com.pingan.pbear.common

import org.scalatest.FlatSpec

/**
  * Created by lovelife on 17/3/5.
  */
class AppConfSpec extends FlatSpec{
  val envmode = AppConf.getEnvMode

  "AppConf" should "get the right text" in {

    val redisHost = AppConf.getRedisHost
    if (List("debug", "dev").contains(envmode)) {
      assert(redisHost === "localhost")
    } else if (envmode == "test") {
      assert(redisHost === "10.31.224.105")
    } else if (envmode == "product") {
      assert(redisHost === "10.33.45.89")
    }
    
  }

}
