package com.pingan.pbear.core.syntax

import org.scalatest.FlatSpec


/**
  * Created by lovelife on 17/3/4.
  */
class SyntaxRegexSpec extends FlatSpec{

  "the create temp table sql syntax" should " test passed" in {
    val createSql = "CREATE TEMPORARY TABLE pbear_test FROM rdb \"key=RD_OUEWR_RD\" select name, age from people where age > 7"
    assertResult(Map("tmptable" -> "pbear_test",
      "dbtype" -> "rdb",
      "option" -> "key=RD_OUEWR_RD",
      "sql" -> "select name, age from people where age > 7"
    )) {
      SyntaxRegex.getCreateTempTableCmdInfo(createSql)
    }
  }

  "the insert sql syntax" should " test passed" in {
    val insertSql = "insert into es 'nodes=localhost|port=9200|index=pbear/news|format=json' select name, age from people"
    assertResult(Map(
      "dbtype" -> "es",
      "option" -> "nodes=localhost|port=9200|index=pbear/news|format=json",
      "sql" -> "select name, age from people"
    )) {
      SyntaxRegex.getInsertTableCmdInfo(insertSql)
    }
  }

  "the set sql syntax" should " test passed" in {
    val setSql = "set es.nodes = localhost"
    assertResult(Map("key"-> "es.nodes", "value"-> "localhost")) {
      SyntaxRegex.getSetCmdInfo(setSql)
    }
  }
}
