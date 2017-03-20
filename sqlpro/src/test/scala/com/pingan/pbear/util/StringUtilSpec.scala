package com.pingan.pbear.util

import org.scalatest.FlatSpec

/**
  * Created by lovelife on 17/3/4.
  */
class StringUtilSpec extends FlatSpec{
  it should "find the table name from a sql" in {
    val sql = "select name, age from people"
    val table = StringUtil.getTableFromSelectSql(sql)
    assert(table === "people")
  }
}
