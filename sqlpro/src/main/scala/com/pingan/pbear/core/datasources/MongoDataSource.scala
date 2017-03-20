package com.pingan.pbear.core.datasources


import com.pingan.pbear.util.StringUtil
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.mongodb.config._
import com.stratio.datasource.util.Config
import org.apache.spark.sql.hive.HiveContext


/**
  * Created by lovelife on 17/2/23.
  */
object MongoDataSource extends DataSource {

  override
  def readFrom(hiveCtx: HiveContext, info: Map[String, String], dfRow: Int): Unit = {

    val tmpTable = info("tmptable")
    val sql = info("sql")
    val table = StringUtil.getTableFromSelectSql(sql)
    val readConfig = creatMongoConf(info)
    hiveCtx.fromMongoDB(readConfig).registerTempTable(table)
    runSqlAndShowResult(hiveCtx, sql, dfRow).registerTempTable(tmpTable)
  }

  override
  def saveTo(hiveCtx: HiveContext, info: Map[String, String], dfRow: Int): Unit = {
    val sql = info("sql")
    val writeConfig = creatMongoConf(info)
    runSqlAndShowResult(hiveCtx, sql, dfRow).saveToMongodb(writeConfig)
  }

  def creatMongoConf(info: Map[String, String]): Config = {
    val option = getMappedOption(info)
    val host = option("host")
    val db = option("database")
    val collection = option("collection")
    var conf = Map(Host -> List(host), Database -> db, Collection -> collection, SamplingRatio -> 1.0, WriteConcern -> "normal")
    if (option.contains("user") && option.contains("password")) {
      conf ++= Map(Credentials -> List(MongodbCredentials(option("user"), db, option("password").toCharArray)))
    }
    val builder = MongodbConfigBuilder(conf)
    val mongoConfig = builder.build()
    mongoConfig
  }
}
