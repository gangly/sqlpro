package com.pingan.pbear.util

import scalikejdbc.ConnectionPool

/**
  * Created by zhangrunqin on 16-12-1.
  */
object SetupJdbc {
  def apply(driver: String, host: String, user: String, password: String): Unit = {
    Class.forName(driver)
    ConnectionPool.singleton(host, user, password)
  }
}
